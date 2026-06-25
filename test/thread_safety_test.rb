require 'test_helper'
require 'securerandom'

# Exercises the two thread-safety hazards in lib/graphiform/core.rb:
#
#   1. Lazy GraphQL class builders (`graphql_type`, `graphql_filter`, etc.)
#      memoize into `@graphql_type` / `@filter` / ... using
#      `unless defined? @foo ... @foo = ...` with no synchronization, and
#      delegate construction to `Helpers.get_const_or_create`, which itself
#      does a check-then-`const_set` without locking. Concurrent callers can
#      each build a *separate* anonymous class, each `const_set` it (last
#      writer wins under the namespace), and each return their own copy to
#      their caller. The class registered under `::Types` ends up out of sync
#      with the class memoized on the model.
#
#   2. The per-model "pending filter/sort/grouping" queues (drained at query
#      time from `arguments` / `own_arguments` singleton overrides) are
#      mutated without locking: two threads can both grab the same `pending`
#      array, both reset `@graphiform_pending_filters` to `[]`, and both
#      iterate. Even when `add_unless_exists` happens to dedupe the final
#      argument names, the work is duplicated and the filter class can be
#      observed mid-build by a concurrent reader.
#
# These tests use a `Gate` to deterministically widen the race window so the
# bug reproduces reliably on a single test run rather than depending on
# scheduler luck.
class ThreadSafetyTest < ActiveSupport::TestCase
  THREAD_COUNT = 8

  # Barrier that releases all `THREAD_COUNT` waiters together.
  class Gate
    def initialize(n)
      @n = n
      @count = 0
      @mutex = Mutex.new
      @cond  = ConditionVariable.new
    end

    def wait
      @mutex.synchronize do
        @count += 1
        if @count >= @n
          @cond.broadcast
        else
          @cond.wait(@mutex) while @count < @n
        end
      end
    end
  end

  def build_model(name)
    klass = Class.new(ApplicationRecord) do
      self.table_name = 'firsts'
    end
    klass.include(Graphiform)
    klass.preferred_name(name)
    klass.graphql_fields :name, :number, :boolean
    klass
  end

  # Instruments `Helpers.get_const_or_create` so that:
  #   * threads sync at `gate` once they've decided the const isn't defined
  #     yet, deterministically widening the natural race window that exists
  #     between the existence check and `const_set`;
  #   * we then delegate to the *real* helper (still racing N threads in
  #     parallel) and count how many times the construction block runs.
  #
  # With unfixed code all N threads run the block; with a correct fix
  # (e.g. a re-checking monitor inside the helper) the block runs exactly
  # once. The gate is placed BEFORE `original.call` so the fix's own
  # mutex never deadlocks against this instrumentation.
  def with_widened_const_race(gate)
    counts = Hash.new(0)
    counts_mutex = Mutex.new
    original = Graphiform::Helpers.method(:get_const_or_create)
    target_consts = []
    target_mutex = Mutex.new

    Graphiform::Helpers.singleton_class.send(:define_method, :get_const_or_create) do |const, mod = Object, &block|
      should_gate = !mod.const_defined?(const, false)
      if should_gate
        target_mutex.synchronize { target_consts << [mod, const] }
        gate.wait
      end
      wrapped = lambda do
        counts_mutex.synchronize { counts[[mod, const]] += 1 }
        # Widen the would-be critical section so concurrent racers reliably
        # overlap on CRuby (GVL releases on sleep). A correct fix serializes
        # entry into this lambda, so the sleep only adds latency once.
        sleep 0.05
        block.call
      end
      original.call(const, mod, &wrapped)
    end
    yield counts
  ensure
    Graphiform::Helpers.singleton_class.send(:define_method, :get_const_or_create, original) if original
  end

  test 'concurrent graphql_connection calls build the connection class exactly once' do
    # `graphql_connection` has no outer `@graphql_connection` memoization;
    # every call funnels through `Helpers.get_const_or_create`. This makes it
    # a clean probe of the unsynchronized check-then-`const_set` window.
    model_name = "TsConn#{SecureRandom.hex(4)}"
    model = build_model(model_name)
    gate = Gate.new(THREAD_COUNT)

    results = Array.new(THREAD_COUNT)
    build_counts = nil
    with_widened_const_race(gate) do |counts|
      build_counts = counts
      threads = THREAD_COUNT.times.map do |i|
        Thread.new { results[i] = model.graphql_connection }
      end
      threads.each(&:join)
    end

    connection_const = "#{model_name}Connection"
    registered = ::Types.const_get(connection_const)

    assert_equal 1, build_counts[[::Types, connection_const]],
      "::Types::#{connection_const} should be constructed exactly once, was " \
      "built #{build_counts[[::Types, connection_const]]} times " \
      "(lazy-builder race in Helpers.get_const_or_create). " \
      "All counts: #{build_counts.inspect}"

    distinct = results.map(&:object_id).uniq
    assert_equal 1, distinct.size,
      "All #{THREAD_COUNT} threads should observe the same connection class, " \
      "got #{distinct.size} distinct class objects."

    assert_same registered, results.first,
      "The class registered under ::Types::#{connection_const} should be the " \
      "same object returned to callers (otherwise resolvers reference a dead class)."
  end

  test 'concurrent graphql_filter calls build the filter class exactly once' do
    model_name = "TsFilter#{SecureRandom.hex(4)}"
    model = build_model(model_name)
    gate = Gate.new(THREAD_COUNT)

    results = Array.new(THREAD_COUNT)
    errors  = []
    error_mutex = Mutex.new
    build_counts = nil

    with_widened_const_race(gate) do |counts|
      build_counts = counts
      threads = THREAD_COUNT.times.map do |i|
        Thread.new do
          begin
            results[i] = model.graphql_filter
            # Touch .arguments so any flush_pending_filters! race fires.
            results[i].arguments
          rescue => e
            error_mutex.synchronize { errors << e }
          end
        end
      end
      threads.each(&:join)
    end

    assert_empty errors,
      "graphql_filter should never raise under concurrent access, got: #{errors.map(&:message).inspect}"

    assert_equal 1, build_counts[[::Inputs::Filters, model_name]],
      "::Inputs::Filters::#{model_name} should be constructed exactly once, " \
      "was built #{build_counts[[::Inputs::Filters, model_name]]} times."

    distinct = results.compact.map(&:object_id).uniq
    assert_equal 1, distinct.size,
      "All #{THREAD_COUNT} threads should observe the same graphql_filter, " \
      "got #{distinct.size} distinct filter classes (lazy-builder + pending-flush race)."

    registered = ::Inputs::Filters.const_get(model_name)
    assert_same registered, results.first,
      "The filter class registered under ::Inputs::Filters::#{model_name} " \
      "should be the same object returned to callers."
  end

  # Even with `Helpers.get_const_or_create` fixed (all racers receive the same
  # filter class), the body of `graphql_filter` in core.rb still relies on
  # `Helpers.add_unless_exists` to dedupe the OR/AND argument additions on
  # the shared class. `add_unless_exists` is check-then-act:
  #
  #   set = tracked_names(klass)
  #   return false if set.include?(normalized)
  #   yield
  #   set << normalized
  #
  # Two threads can both observe `set.include?(...) == false`, both `yield`
  # the `argument 'OR', [self]` class_eval, and both `set << normalized`.
  # graphql-ruby silently overwrites the duplicate argument today; future
  # versions may raise. This test exercises the primitive directly so the
  # race is reproducible regardless of how `graphql_filter` is structured.
  test 'concurrent Helpers.add_unless_exists must yield the block exactly once per (klass, name)' do
    klass = Class.new
    name  = 'OR'
    gate  = Gate.new(THREAD_COUNT)

    yield_count = 0
    yield_mutex = Mutex.new
    original = Graphiform::Helpers.method(:add_unless_exists)

    Graphiform::Helpers.singleton_class.send(:define_method, :add_unless_exists) do |klass_arg, name_arg, &block|
      wrapped = proc do
        yield_mutex.synchronize { yield_count += 1 }
        sleep 0.05
        block.call
      end
      gate.wait
      original.call(klass_arg, name_arg, &wrapped)
    end

    results = []
    results_mutex = Mutex.new
    threads = THREAD_COUNT.times.map do
      Thread.new do
        ran = Graphiform::Helpers.add_unless_exists(klass, name) { :did_work }
        results_mutex.synchronize { results << ran }
      end
    end
    threads.each(&:join)

    assert_equal 1, yield_count,
      "add_unless_exists block yielded #{yield_count} times across #{THREAD_COUNT} threads; " \
      "should be exactly 1 (check-then-act race on the tracked-names Set)."
    assert_equal 1, results.count(true),
      "exactly one thread should observe add_unless_exists returning true; got #{results.count(true)}. " \
      "Result vector: #{results.inspect}"
  ensure
    Graphiform::Helpers.singleton_class.send(:define_method, :add_unless_exists, original) if original
  end

  # `flush_pending_filters!` (core.rb:328) reads then clears
  # `@graphiform_pending_filters` without synchronization:
  #
  #   pending = @graphiform_pending_filters
  #   @graphiform_pending_filters = []
  #   pending.each { |e| graphql_add_scopes_to_filter(*e) }
  #
  # Two threads can both grab the same `pending` array reference before either
  # clears the ivar, then both iterate and call graphql_add_scopes_to_filter
  # for every entry — duplicating the wiring work and (downstream) racing
  # add_unless_exists yet again.
  #
  # CRuby's GVL normally keeps the read+clear window invisible. We use a
  # TracePoint on the exact line of the clear to force a thread switch
  # between the read and the clear so the race manifests deterministically.
  test 'concurrent flush_pending_filters! must not double-process pending entries' do
    model = build_model("TsFlush#{SecureRandom.hex(4)}")
    # graphql_fields populated @graphiform_pending_filters with one entry per
    # column declared in build_model. Snapshot the identifiers so we can
    # assert each is processed exactly once.
    pending = model.instance_variable_get(:@graphiform_pending_filters).dup
    refute_empty pending, 'fixture expected pending filters from graphql_fields'
    identifiers = pending.map { |(_n, id, _o)| id }

    call_counts = Hash.new(0)
    counts_mutex = Mutex.new
    original_add = model.method(:graphql_add_scopes_to_filter)
    model.define_singleton_method(:graphql_add_scopes_to_filter) do |name, identifier, **options|
      counts_mutex.synchronize { call_counts[identifier] += 1 }
      original_add.call(name, identifier, **options)
    end

    core_path = File.expand_path('../lib/graphiform/core.rb', __dir__)
    # core.rb:332 is `@graphiform_pending_filters = []` — the clear that
    # creates the read/clear race window. TracePoint :line fires BEFORE the
    # line runs, so sleep here is between `pending = @gpf` and `@gpf = []`.
    trace = TracePoint.new(:line) do |tp|
      if tp.path == core_path && tp.lineno == 332 && tp.method_id == :flush_pending_filters!
        sleep 0.02
      end
    end

    gate = Gate.new(THREAD_COUNT)
    errors = []
    errors_mutex = Mutex.new

    trace.enable
    begin
      threads = THREAD_COUNT.times.map do
        Thread.new do
          gate.wait
          begin
            model.flush_pending_filters!
          rescue StandardError => e
            errors_mutex.synchronize { errors << e }
          end
        end
      end
      threads.each(&:join)
    ensure
      trace.disable
    end

    assert_empty errors,
      "flush_pending_filters! should not raise under concurrent access, got: #{errors.map(&:message).inspect}"

    identifiers.each do |identifier|
      assert_equal 1, call_counts[identifier],
        "#{identifier} should be processed exactly once across all threads, " \
        "was processed #{call_counts[identifier]} times (read-then-clear race in flush_pending_filters!)."
    end
  end
end
