using System.Net;
using DotNext.IO.Log;
using DotNext.Net.Cluster.Consensus.Raft.NetworkTransport;

namespace DotNext.Net.Cluster.Consensus.Raft;

// TEMPORARY PROBE - does the membership check actually stop an adversary?
[Collection(TestCollections.Raft)]
public sealed class ProbeSpoofTests : RaftTest
{
    private const int HostPort = 3601;

    [Fact]
    public static async Task MemberIdIsDerivableFromPublicEndPoint()
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

        // An outsider knows only the endpoint - which is public, it is how you connect.
        var derived = ClusterMemberId.FromEndPoint(new IPEndPoint(IPAddress.Loopback, HostPort));

        // It is byte-identical to the real member id. No secret is involved.
        Equal(local.Id, derived);

        await host.StopAsync(TestToken);
    }

    [Fact]
    public static async Task SpoofedMemberIdPassesMembershipCheck()
    {
        await using var host = new RaftCluster(
            new RaftCluster.TcpConfiguration(new IPEndPoint(IPAddress.Loopback, HostPort + 1))
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
        var term = host.AuditTrail.Term;

        // The adversary computes a valid member id from the public endpoint
        // instead of using a random one. Same equal-term request otherwise.
        var spoofed = ClusterMemberId.FromEndPoint(new IPEndPoint(IPAddress.Loopback, HostPort + 1));

        var result = await local.AppendEntriesAsync(
            spoofed,
            term,
            new LogEntryProducer<EmptyLogEntry>([new EmptyLogEntry { Term = term }]),
            lastIndex,
            prevLogTerm,
            lastIndex + 1L,
            local.Version,
            TestToken);

        // If the membership check were a real defense, this would be Rejected.
        Equal(HeartbeatResult.ReplicatedWithLeaderTerm, result.Value.Result);
        Equal(lastIndex + 1L, host.AuditTrail.LastEntryIndex);
        Equal(lastIndex + 1L, host.AuditTrail.LastCommittedEntryIndex);

        await host.StopAsync(TestToken);
    }
}
