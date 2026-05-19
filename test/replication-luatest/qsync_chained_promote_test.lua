local t = require('luatest')
local vraft = require('test.replication-luatest.lib.vraft_tools')

local g = t.group()

-- Every wait in the file. Nothing here is expected to take long; a wait
-- running out is a failure, not slowness.
local TIMEOUT = 10

--
-- A newly elected leader holding somebody's pending PROMOTE chains its own on
-- top: the new PROMOTE inherits the previous owner, the confirm boundary and
-- the confirmed state of the pending one, and the pending one's own row with
-- them. Whichever of the two CONFIRMs applies first on a node, and whether the
-- other ever does, the node ends in the same state.
--
local function build_replica_set(cg, node_count, quorum)
    vraft.build_replica_set(cg, {
        replication_timeout = 0.1,
        replication_synchro_quorum = quorum,
        replication_synchro_timeout = TIMEOUT,
        election_mode = 'manual',
    }, {node_count = node_count, glob_timeout = TIMEOUT})
end

-- The error injections and the debug-only box.info.synchro introspection.
g.before_all(function()
    t.tarantool.skip_if_not_debug()
end)

g.after_each(function(cg)
    for _, node in pairs(cg.nodes) do
        pcall(node.exec, node, function()
            box.error.injection.set('ERRINJ_WAL_DELAY_COUNTDOWN', -1)
            box.error.injection.set('ERRINJ_WAL_DELAY', false)
        end)
    end
    cg.replica_set:drop()
end)

--
-- The base of a chain is slow with its CONFIRM; the chain gets confirmed
-- first. The base's late CONFIRM finds its row confirmed already - the chain
-- carried it in its confirmed vclock - and is a nop everywhere: not a split
-- brain, not a second application. A healthy majority cluster.
--
g.test_late_confirm_of_chain_base_is_nop = function(cg)
    build_replica_set(cg, 3, 2)
    local node1, node2, node3 = cg.nodes[1], cg.nodes[2], cg.nodes[3]
    local node2_id, node3_id = node2:get_instance_id(), node3:get_instance_id()

    node1:exec(function()
        box.ctl.promote()
        t.assert_equals(box.info.synchro.queue.owner, box.info.id)
        box.schema.space.create('s', {is_sync = true}):create_index('p')
        box.space.s:replace{1}
    end)
    vraft.wait_sync(cg)
    -- Node 2's PROMOTE gathers its quorum and replicates everywhere, the
    -- CONFIRM is stuck.
    vraft.promote_start_blocked_at_confirm(node2, 'promote2')
    local term2 = node2:get_election_term()
    local promote2_lsn = node2:exec(function()
        return box.info.synchro.own_promote.lsn
    end)
    node1:wait_for_vclock_of(node2)
    node3:wait_for_vclock_of(node2)
    -- Node 3 chains over the pending PROMOTE of node 2 and confirms. The chain
    -- covers node 2's promotion: its term is applied, its slot is dropped,
    -- node 2 gives its own promotion up.
    node3:exec(function(node2_id, term2)
        t.assert_equals(box.info.synchro.nodes[node2_id].promote.term, term2)
        box.ctl.promote()
        t.assert_equals(box.info.synchro.queue.owner, box.info.id)
        t.assert_equals(box.info.synchro.term_map[node2_id], term2)
        t.assert_equals(box.info.synchro.nodes[node2_id].promote, nil)
    end, {node2_id, term2})
    local term3 = node3:get_election_term()
    t.assert_lt(term2, term3)
    node1:wait_for_vclock_of(node3)
    node1:exec(function(node2_id, node3_id)
        t.assert_equals(box.info.synchro.queue.owner, node3_id)
        t.assert_equals(box.info.synchro.nodes[node2_id].promote, nil)
    end, {node2_id, node3_id})
    -- Node 2's WAL is released. Node 2 has seen node 3's term in the Raft
    -- messages meanwhile and gave its promotion up, but the CONFIRM is in the
    -- WAL queue already and lands. It is first in node 2's own WAL, so node 2
    -- applies its own promotion, and node 3's chain over it right after. On
    -- the other nodes the CONFIRM is late: its row is confirmed already by the
    -- chain, and it is a nop.
    local ok, err = vraft.promote_join_at_wal_release(node2, 'promote2')
    t.assert_not(ok)
    t.assert_equals(err.code, box.error.INTERFERING_ELECTIONS)
    vraft.wait_sync(cg)
    for _, node in pairs(cg.nodes) do
        node:exec(function(node2_id, node3_id, term2, term3, promote2_lsn)
            t.assert_equals(box.info.synchro.queue.owner, node3_id)
            t.assert_equals(box.info.synchro.queue.term, term3)
            t.assert_equals(box.info.synchro.term_map[node2_id], term2)
            t.assert_le(promote2_lsn,
                        box.info.synchro.confirmed_vclock[node2_id])
            t.assert_equals(box.info.synchro.nodes[node2_id].promote, nil)
            t.assert_equals(box.space.s:select(), {{1}})
        end, {node2_id, node3_id, term2, term3, promote2_lsn})
        vraft.assert_all_upstreams_follow(node)
    end
end
