using System.Net;
using DotNext.IO.Log;
using DotNext.Net.Cluster.Consensus.Raft.NetworkTransport;

namespace DotNext.Net.Cluster.Consensus.Raft;

// SUPERVISOR COMMENT 2 - recovery of a node with an outdated membership configuration.
//
// Scenario: C is offline while the cluster adds D. D later becomes leader.
// C comes back holding a configuration that contains C but not D.
// C must accept D's replication, because the configuration entry that would
// teach C about D is itself delivered by AppendEntries.
[Collection(TestCollections.Raft)]
public sealed class ProbeRecoveryTests : RaftTest
{
    private const int HostPort = 3611;

    // A node that is in its own configuration, but does not know the current leader.
    private static async Task<RaftCluster> StartStaleNodeAsync(int port)
    {
        var host = new RaftCluster(
            new RaftCluster.TcpConfiguration(new IPEndPoint(IPAddress.Loopback, port))
            {
                ColdStart = true,
                ConfigurationStorage = null,
            })
        { AuditTrail = new ConsensusOnlyState() };

        await host.StartAsync(TestToken);
        await host.WaitForLeadershipAsync(TestToken);
        return host;
    }

    // The most favourable case for recovery: higher term, and a prevLogIndex C already matches.
    // If C cannot accept even this, it can never learn the configuration that contains D.
    [Fact]
    public static async Task StaleNodeAcceptsUnknownHigherTermLeader()
    {
        await using var host = await StartStaleNodeAsync(HostPort);
        ILocalMember local = host;

        var lastIndex = host.AuditTrail.LastEntryIndex;
        var prevLogTerm = await host.AuditTrail.GetTermAsync(lastIndex, TestToken);
        var leaderTerm = host.AuditTrail.Term + 5L;

        var result = await local.AppendEntriesAsync(
            new ClusterMemberId(Random.Shared),
            leaderTerm,
            new LogEntryProducer<EmptyLogEntry>([new EmptyLogEntry { Term = leaderTerm }]),
            lastIndex,
            prevLogTerm,
            lastIndex + 1L,
            local.Version,
            TestToken);

        Equal(HeartbeatResult.ReplicatedWithLeaderTerm, result.Value.Result);
        Equal(lastIndex + 1L, host.AuditTrail.LastEntryIndex);

        await host.StopAsync(TestToken);
    }

    // Real recovery backtracks. C is behind, so the leader's first probe misses C's log.
    // That failed probe can still raise C's term; the retry then arrives at an equal term.
    [Fact]
    public static async Task StaleNodeRecoversAfterFailedProbe()
    {
        await using var host = await StartStaleNodeAsync(HostPort + 1);
        ILocalMember local = host;

        var unknownLeader = new ClusterMemberId(Random.Shared);
        var lastIndex = host.AuditTrail.LastEntryIndex;
        var prevLogTerm = await host.AuditTrail.GetTermAsync(lastIndex, TestToken);
        var leaderTerm = host.AuditTrail.Term + 5L;

        // First probe: prevLogIndex is beyond C's log, so it cannot match.
        var probe = await local.AppendEntriesAsync(
            unknownLeader,
            leaderTerm,
            new LogEntryProducer<EmptyLogEntry>([new EmptyLogEntry { Term = leaderTerm }]),
            lastIndex + 10L,
            prevLogTerm,
            lastIndex + 11L,
            local.Version,
            TestToken);

        Equal(HeartbeatResult.Rejected, probe.Value.Result);

        // Retry after backtracking, with a prevLogIndex C actually has.
        var retry = await local.AppendEntriesAsync(
            unknownLeader,
            leaderTerm,
            new LogEntryProducer<EmptyLogEntry>([new EmptyLogEntry { Term = leaderTerm }]),
            lastIndex,
            prevLogTerm,
            lastIndex + 1L,
            local.Version,
            TestToken);

        Equal(HeartbeatResult.ReplicatedWithLeaderTerm, retry.Value.Result);
        Equal(lastIndex + 1L, host.AuditTrail.LastEntryIndex);

        await host.StopAsync(TestToken);
    }
}
