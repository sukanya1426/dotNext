using System.Net;
using DotNext.IO.Log;
using DotNext.Net.Cluster.Consensus.Raft.NetworkTransport;

namespace DotNext.Net.Cluster.Consensus.Raft;

// Recovery of a node that already stepped down to the leader's term, then restarted.
//
// C accepted a higher-term AppendEntries from leader D and stepped down to D's term,
// but crashed before the configuration entry naming D was committed and applied.
// On restart C reloads term == D's term, and its persisted configuration still lacks D.
// D's next contact therefore arrives at an EQUAL term from a sender C does not know.
//
// NOTE: the input tuple here (unknown sender, senderTerm == local term, matching
// prevLogIndex, local node present in its own configuration) is exactly the tuple
// AppendEntriesMembershipTests.RejectsAppendEntriesFromNonMember asserts must be
// Rejected. Recovery requires it to be accepted. Both cannot hold.
[Collection(TestCollections.Raft)]
public sealed class ProbeContradictionTests : RaftTest
{
    private const int HostPort = 3621;

    [Fact]
    public static async Task RestartedNodeAcceptsEqualTermUnknownLeader()
    {
        await using var host = new RaftCluster(
            new RaftCluster.TcpConfiguration(new IPEndPoint(IPAddress.Loopback, HostPort))
            {
                ColdStart = true,
                ConfigurationStorage = null,
            })
        { AuditTrail = new ConsensusOnlyState() };

        await host.StartAsync(TestToken);
        await host.WaitForLeadershipAsync(TestToken);
        ILocalMember local = host;

        var lastIndex = host.AuditTrail.LastEntryIndex;
        var prevLogTerm = await host.AuditTrail.GetTermAsync(lastIndex, TestToken);

        // Equal term: C already stepped down to this term before restarting.
        var leaderTerm = host.AuditTrail.Term;

        var result = await local.AppendEntriesAsync(
            new ClusterMemberId(Random.Shared),
            leaderTerm,
            new LogEntryProducer<EmptyLogEntry>([new EmptyLogEntry { Term = leaderTerm }]),
            lastIndex,
            prevLogTerm,
            lastIndex + 1L,
            local.Version,
            TestToken);

        // Without this, C can never receive the configuration entry naming D.
        Equal(HeartbeatResult.ReplicatedWithLeaderTerm, result.Value.Result);
        Equal(lastIndex + 1L, host.AuditTrail.LastEntryIndex);

        await host.StopAsync(TestToken);
    }
}
