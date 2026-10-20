namespace ChannelsPipeline.Tests;

public static class Limits
{
    /// <summary>Per-test timeout so that a hang fails instead of blocking the run.</summary>
    public const int Ms = 60_000;
}

public class OptionsTests
{
    private static PipelineBuilder<int> P() => Pipeline.From(new[] { 1, 2, 3 });

    [Fact(Timeout = Limits.Ms)]
    public void StageOptions_Defaults()
    {
        var o = new StageOptions();
        Assert.Equal(16, o.Capacity);
        Assert.Equal(1, o.Parallelism);
        Assert.True(o.Ordered);
        Assert.Same(RetryPolicy.None, o.Retry);
        Assert.Null(o.ItemTimeout);
        Assert.Null(o.ReorderWindow);
    }

    [Theory(Timeout = Limits.Ms)]
    [InlineData(1, 4)]
    [InlineData(2, 8)]
    [InlineData(8, 32)]
    public void ReorderWindow_DefaultsToFourTimesParallelism(int parallelism, int expected) =>
        Assert.Equal(expected, new StageOptions { Parallelism = parallelism }.EffectiveReorderWindow);

    [Fact(Timeout = Limits.Ms)]
    public void ReorderWindow_ExplicitValueWins() =>
        Assert.Equal(5, new StageOptions { Parallelism = 2, ReorderWindow = 5 }.EffectiveReorderWindow);

    [Fact(Timeout = Limits.Ms)]
    public void NeedsReordering_OnlyForOrderedWithManyWorkers()
    {
        Assert.False(new StageOptions { Ordered = true, Parallelism = 1 }.NeedsReordering);
        Assert.False(new StageOptions { Ordered = false, Parallelism = 4 }.NeedsReordering);
        Assert.True(new StageOptions { Ordered = true, Parallelism = 2 }.NeedsReordering);
    }

    [Theory(Timeout = Limits.Ms)]
    [InlineData(0)]
    [InlineData(-1)]
    public void Stage_RejectsNonPositiveCapacity(int capacity) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => P().Stage("a", x => x, new StageOptions { Capacity = capacity }));

    [Theory(Timeout = Limits.Ms)]
    [InlineData(0)]
    [InlineData(-3)]
    public void Stage_RejectsNonPositiveParallelism(int parallelism) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => P().Stage("a", x => x, new StageOptions { Parallelism = parallelism }));

    [Fact(Timeout = Limits.Ms)]
    public void Stage_RejectsWindowSmallerThanParallelism() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => P().Stage("a", x => x, new StageOptions { Parallelism = 4, ReorderWindow = 3 }));

    [Fact(Timeout = Limits.Ms)]
    public void Stage_AcceptsWindowEqualToParallelism() =>
        P().Stage("a", x => x, new StageOptions { Parallelism = 4, ReorderWindow = 4 });

    [Theory(Timeout = Limits.Ms)]
    [InlineData(0)]
    [InlineData(-5)]
    public void Stage_RejectsNonPositiveTimeout(int ms) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => P().Stage("a", x => x, new StageOptions { ItemTimeout = TimeSpan.FromMilliseconds(ms) }));

    [Fact(Timeout = Limits.Ms)]
    public void Stage_RejectsDuplicateNames() =>
        Assert.Throws<ArgumentException>(() => P().Stage("a", x => x).Stage("a", x => x));

    [Fact(Timeout = Limits.Ms)]
    public void Stage_ReservesSinkName() =>
        Assert.Throws<ArgumentException>(() => P().Stage("sink", x => x));

    [Theory(Timeout = Limits.Ms)]
    [InlineData("")]
    [InlineData("  ")]
    public void Stage_RejectsBlankName(string name) =>
        Assert.Throws<ArgumentException>(() => P().Stage(name, x => x));

    [Fact(Timeout = Limits.Ms)]
    public void Stage_RejectsNullFunction() =>
        Assert.Throws<ArgumentNullException>(() => P().Stage<int>("a", (Func<int, int>)null!));

    [Fact(Timeout = Limits.Ms)]
    public void From_RejectsNullSource() =>
        Assert.Throws<ArgumentNullException>(() => Pipeline.From((IAsyncEnumerable<int>)null!));

    [Fact(Timeout = Limits.Ms)]
    public void From_RejectsInvalidSinkOptions() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => Pipeline.From(new[] { 1 }, new PipelineOptions { SinkOptions = new StageOptions { Capacity = 0 } }));

    [Fact(Timeout = Limits.Ms)]
    public void From_RejectsNegativeDeadLetterCap() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => Pipeline.From(new[] { 1 }, new PipelineOptions { MaxReportedDeadLetters = -1 }));

    [Fact(Timeout = Limits.Ms)]
    public void StageNames_AreReportedInOrder() =>
        Assert.Equal(new[] { "a", "b", "c" }, P().Stage("a", x => x).Stage("b", x => x).Stage("c", x => x).StageNames);

    [Fact(Timeout = Limits.Ms)]
    public void RetryPolicy_RejectsZeroAttempts() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new RetryPolicy(0));

    [Fact(Timeout = Limits.Ms)]
    public void RetryPolicy_None_IsSingleAttemptWithoutDelay()
    {
        Assert.Equal(1, RetryPolicy.None.MaxAttempts);
        Assert.Equal(TimeSpan.Zero, RetryPolicy.None.Backoff(1));
        Assert.True(RetryPolicy.None.ShouldRetry(new Exception()));
    }

    [Fact(Timeout = Limits.Ms)]
    public void RetryPolicy_Fixed_UsesSameDelay()
    {
        var p = RetryPolicy.Fixed(4, TimeSpan.FromSeconds(3));
        Assert.Equal(4, p.MaxAttempts);
        Assert.All(new[] { 1, 2, 3 }, a => Assert.Equal(TimeSpan.FromSeconds(3), p.Backoff(a)));
    }

    [Fact(Timeout = Limits.Ms)]
    public void RetryPolicy_Exponential_DoublesAndCaps()
    {
        var p = RetryPolicy.Exponential(10, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10));
        Assert.Equal(TimeSpan.FromSeconds(1), p.Backoff(1));
        Assert.Equal(TimeSpan.FromSeconds(2), p.Backoff(2));
        Assert.Equal(TimeSpan.FromSeconds(4), p.Backoff(3));
        Assert.Equal(TimeSpan.FromSeconds(8), p.Backoff(4));
        Assert.Equal(TimeSpan.FromSeconds(10), p.Backoff(5));
    }

    [Fact(Timeout = Limits.Ms)]
    public void RetryPolicy_Exponential_DoesNotOverflow()
    {
        var p = RetryPolicy.Exponential(500, TimeSpan.FromSeconds(1));
        Assert.True(p.Backoff(400) > TimeSpan.Zero);
    }
}
