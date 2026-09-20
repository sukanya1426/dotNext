using System.Net;
using DotNext.IO.Log;
using DotNext.Net.Cluster.Consensus.Raft.NetworkTransport;

namespace DotNext.Net.Cluster.Consensus.Raft;

[Collection(TestCollections.Raft)]
public sealed class AppendEntriesMembershipTests : RaftTest
{
    private const int HostPort = 3562;

    [Fact]
    public static async Task RejectsAppendEntriesFromNonMember()
    {
        await using var host = await StartLeaderAsync();
        var before = await CaptureAsync(host);

        var result = await AppendAsync(host, new ClusterMemberId(Random.Shared), before.Term, before);

        Equal(HeartbeatResult.Rejected, result.Value.Result);
        Equal(before.LastIndex, host.AuditTrail.LastEntryIndex);
        Equal(before.CommitIndex, host.AuditTrail.LastCommittedEntryIndex);
        Equal(before.Term, host.AuditTrail.Term);

        await host.StopAsync(TestToken);
    }

    [Fact]
    public static async Task AcceptsAppendEntriesFromClusterMember()
    {
        await using var host = await StartLeaderAsync();
        var before = await CaptureAsync(host);
        ILocalMember local = host;

        var result = await AppendAsync(host, local.Id, before.Term, before);

        Equal(HeartbeatResult.ReplicatedWithLeaderTerm, result.Value.Result);
        Equal(before.LastIndex + 1L, host.AuditTrail.LastEntryIndex);
        Equal(before.LastIndex + 1L, host.AuditTrail.LastCommittedEntryIndex);

        await host.StopAsync(TestToken);
    }

    [Fact]
    public static async Task RejectsHigherTermAppendEntriesFromNonMemberWithoutUpdatingTerm()
    {
        await using var host = await StartLeaderAsync();
        var before = await CaptureAsync(host);
        var outsiderTerm = before.Term + 5L;

        var result = await AppendAsync(host, new ClusterMemberId(Random.Shared), outsiderTerm, before);

        Equal(HeartbeatResult.Rejected, result.Value.Result);
        Equal(before.LastIndex, host.AuditTrail.LastEntryIndex);
        Equal(before.CommitIndex, host.AuditTrail.LastCommittedEntryIndex);
        Equal(before.Term, host.AuditTrail.Term);
        False(host.LeadershipToken.IsCancellationRequested);

        await host.StopAsync(TestToken);
    }

    [Fact]
    public static async Task FrozenJoinerAcceptsCatchUpFromUnknownLeader()
    {
        await using var host = new RaftCluster(CreateConfiguration(coldStart: false)) { AuditTrail = new ConsensusOnlyState() };
        await host.StartAsync(TestToken);
        False(host.Readiness.IsCompletedSuccessfully);

        var before = await CaptureAsync(host);
        var leaderTerm = before.Term + 1L;

        var result = await AppendAsync(host, new ClusterMemberId(Random.Shared), leaderTerm, before);

        Equal(HeartbeatResult.ReplicatedWithLeaderTerm, result.Value.Result);
        Equal(before.LastIndex + 1L, host.AuditTrail.LastEntryIndex);
        Equal(before.LastIndex + 1L, host.AuditTrail.LastCommittedEntryIndex);

        await host.StopAsync(TestToken);
    }

    private static async Task<RaftCluster> StartLeaderAsync()
    {
        var host = new RaftCluster(CreateConfiguration()) { AuditTrail = new ConsensusOnlyState() };
        await host.StartAsync(TestToken);
        await host.WaitForLeadershipAsync(TestToken);
        return host;
    }

    private readonly record struct LogSnapshot(long LastIndex, long CommitIndex, long Term, long PrevLogTerm);

    private static async ValueTask<LogSnapshot> CaptureAsync(RaftCluster host)
        => new(
            host.AuditTrail.LastEntryIndex,
            host.AuditTrail.LastCommittedEntryIndex,
            host.AuditTrail.Term,
            await host.AuditTrail.GetTermAsync(host.AuditTrail.LastEntryIndex, TestToken));

    private static ValueTask<Result<ReplicationStatus>> AppendAsync(RaftCluster host, ClusterMemberId sender, long senderTerm, LogSnapshot before)
    {
        ILocalMember local = host;
        return local.AppendEntriesAsync(
            sender,
            senderTerm,
            new LogEntryProducer<EmptyLogEntry>([new EmptyLogEntry { Term = senderTerm }]),
            before.LastIndex,
            before.PrevLogTerm,
            before.LastIndex + 1L,
            local.Version,
            TestToken);
    }

    private static RaftCluster.TcpConfiguration CreateConfiguration(bool coldStart = true)
        => new(new IPEndPoint(IPAddress.Loopback, HostPort))
        {
            ColdStart = coldStart,
            ConfigurationStorage = null,
        };
}
