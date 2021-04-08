module RZ.FSharp.Akka.Streams.SourceExtensions

open System
open System.Threading.Tasks
open Akka.Streams
open Akka.Streams.Dsl
open System.Collections.Generic
open Akka.Event

type Source<'Out,'Mat> with
    member inline this.aggregate(zero: 'Result, mapper: 'Result -> 'Out -> 'Result) =
        this.Aggregate(zero, fun last item -> mapper last item)

    member inline this.aggregateAsync(zero: 'Result, mapper: 'Result -> 'Out -> Task<'Result>) =
        this.AggregateAsync(zero, fun last item -> mapper last item)

    member inline this.asSourceWithContext(fn: 'Out -> 'Context) =
        this.AsSourceWithContext(fun item -> fn item)

    member inline this.batch(max, seed: 'Out -> 'Out2, aggregate: 'Out2 -> 'Out -> 'Out2) =
        this.Batch(max, (fun item -> seed item), fun last item -> aggregate last item)

    member inline this.batchWeighted(max,costFunction: 'Out -> int64, seed: 'Out -> 'Out2, aggregate: 'Out2 -> 'Out -> 'Out2) =
        this.BatchWeighted(max, (fun item -> costFunction item), (fun item -> seed item), fun last item -> aggregate last item)

    member inline this.collect(isDefined: 'Out -> bool, collection: 'Out -> 'Out2) =
        this.Collect((fun item -> isDefined item), fun item -> collection item)

    member inline this.combine(first, second, strategy: int -> IGraph<UniformFanInShape<'Out,'c>,Akka.NotUsed>, [<ParamArray>] rest) =
        this.Combine(first, second, (fun i -> strategy i), rest)

    member inline this.combineMaterialized(other, strategy: int -> IGraph<UniformFanInShape<'Out,'Out2>,Akka.NotUsed>, combineMaterializer: 'Mat -> 'Mat2 -> 'Mat3) =
        this.CombineMaterialized(other, (fun item -> strategy item), (fun mat1 mat2 -> combineMaterializer mat1 mat2))

    member inline this.concatMany(flatten: 'Out -> IGraph<SourceShape<'T>,'Mat>) =
        this.ConcatMany(fun item -> flatten item)

    member inline this.concatMaterialized(that, materializedFunction: 'Mat -> 'Mat2 -> 'Mat3) =
        this.ConcatMaterialized(that, fun m1 m2 -> materializedFunction m1 m2)

    member inline this.conflate(aggregate: 'Out -> 'Out -> 'Out) =
        this.Conflate(fun last item -> aggregate last item)

    member inline this.conflateWithSeed(seed: 'Out -> 'Seed, aggregate: 'Seed -> 'Out -> 'Seed) =
        this.ConflateWithSeed((fun item -> seed item), fun last item -> aggregate last item)

    member inline this.expand(extrapolate: 'Out -> IEnumerator<'T>) =
        this.Expand(fun item -> extrapolate item)
        
    member inline this.groupBy(maxSubstreams, groupingFunction: 'Out -> 'Key) =
        this.GroupBy(maxSubstreams, fun i -> groupingFunction i)

    member inline this.interleaveMaterialized(graph, segmentSize, combine: 'Mat -> 'Mat2 -> 'Mat3) =
        this.InterleaveMaterialized(graph, segmentSize, fun m m2 -> combine m m2)
    
    member inline this.keepAlive(timeout, injectElement: unit -> 'Out) =
        this.KeepAlive(timeout, fun () -> injectElement())

    member inline this.limitWeighted(max, costFunc: 'Out -> int64) =
        this.LimitWeighted(max, fun item -> costFunc item)

    member inline this.log(name, extract: ('Out -> obj) option, log: ILoggingAdapter option) =
        this.Log( name,
                  (extract |> Option.map (fun f -> Func<'Out,obj>(f)) |> Option.defaultValue null),
                  log |> Option.defaultValue null)

    member inline this.mapMaterializedValue(mapFunc: 'Mat -> 'Mat2) =
        this.MapMaterializedValue(fun m -> mapFunc m)

    member inline this.mergeMany(breadth, flatten: 'Out -> IGraph<SourceShape<'Out2>, 'Mat>) =
        this.MergeMany(breadth, fun i -> flatten i)

    member inline this.mergeMaterialized(that, combine: 'Mat -> 'Mat2 -> 'Mat3) =
        this.MergeMaterialized(that, fun m m2 -> combine m m2)

    member inline this.mergeSorted(other, orderFunc: 'Out -> 'Out -> int) =
        this.MergeSorted(other, fun u v -> orderFunc u v)

    member inline this.monitor(combine: 'Mat -> IFlowMonitor -> 'Mat2) =
        this.Monitor(fun m monitor -> combine m monitor)

    member inline this.orElseMaterialized(secondary, materializedFunction: 'Mat -> 'Mat2 -> 'Mat3) =
        this.OrElseMaterialized(secondary, fun m m2 -> materializedFunction m m2)

    member inline this.recover(partialFunc: exn -> 'Out option) =
        this.Recover(fun ex -> match partialFunc ex with
                               | Some v -> Akka.Util.Option(v)
                               | None -> Akka.Util.Option.None)

    member inline this.recoverWithRetries(partialFuncs: exn -> IGraph<SourceShape<'Out>,'Mat>, attempt) =
        this.RecoverWithRetries((fun ex -> partialFuncs ex), attempt)

    member inline this.runAggregate(zero: 'Result, mapper: 'Result -> 'Out -> 'Result, materializer) =
        async { return! this.RunAggregate(zero, (fun last item -> mapper last item), materializer) |> Async.AwaitTask }

    member inline this.runAggregateAsync(zero: 'Result, mapper: 'Result -> 'Out -> Task<'Result>, materializer) =
        async { return! this.RunAggregateAsync(zero, (fun last item -> mapper last item), materializer) |> Async.AwaitTask }

    member inline this.runForeach(action: 'Out -> unit, materializer) =
        async { return! this.RunForeach((fun result -> action result), materializer) |> Async.AwaitTask }

    member inline this.runSum(reduce: 'Out -> 'Out -> 'Out, materializer) =
        async { return! this.RunSum((fun last item -> reduce last item), materializer) |> Async.AwaitTask }

    member inline this.select(mapper: 'Out -> 'c) =
        this.Select(fun x -> mapper(x))

    member inline this.selectAsync(numParallel, mapper: 'Out -> Async<'c>) =
        this.SelectAsync(numParallel,fun x -> mapper(x) |> Async.StartAsTask)

    member inline this.scan(zero: 'Result, mapper: 'Result -> 'Out -> 'Result) =
        this.Scan(zero, fun last item -> mapper last item)

    member inline this.scanAsync(zero: 'Result, mapper: 'Result -> 'Out -> Task<'Result>) =
        this.ScanAsync(zero, fun last item -> mapper last item)

    member inline this.selectAsyncUnordered(parallelism, asyncMapper: 'Out -> Task<'NewOut>) =
        this.SelectAsyncUnordered(parallelism, fun i -> asyncMapper i)

    member inline this.selectError(selector: exn -> exn) =
        this.SelectError(fun ex -> selector ex)

    member inline this.selectMany(mapConcater: 'Out -> 'NewOut seq) =
        this.SelectMany(fun i -> mapConcater i)

    member inline this.skipWhile(predicate: 'Out -> bool) =
        this.SkipWhile(fun i -> predicate i)

    member inline this.splitAfter(predicate: 'Out -> bool) =
        this.SplitAfter(fun i -> predicate i)

    member inline this.splitAfter(substreamCancelStrategy, predicate: 'Out -> bool) =
        this.SplitAfter(substreamCancelStrategy, fun i -> predicate i)

    member inline this.splitWhen(predicate: 'Out -> bool) =
        this.SplitWhen(fun i -> predicate i)

    member inline this.splitWhen(substreamCancelStrategy, predicate: 'Out -> bool) =
        this.SplitWhen(substreamCancelStrategy, fun i -> predicate i)

    member inline this.statefulSelectMany(mapConcaterFactory: unit -> 'Out -> 'NewOut seq) =
        this.StatefulSelectMany(fun _ -> Func<'Out,'NewOut seq>(mapConcaterFactory()))

    member inline this.sum(reduce: 'Out -> 'Out -> 'Out) =
        this.Sum(fun last i -> reduce last i)

    member inline this.takeWhile(predicate: 'Out -> bool, inclusive) =
        this.TakeWhile((fun i -> predicate i), inclusive)

    member inline this.throttle(cost, per, minimumBurst, calculateCost: 'Out -> int, mode) =
        this.Throttle(cost, per, minimumBurst, (fun i -> calculateCost i), mode)

    member inline this.watchTermination(materializerFunction: 'Mat -> Task -> 'Mat2) =
        this.WatchTermination(fun m t -> materializerFunction m t)

    member inline this.where(predicate: 'Out -> bool) =
        this.Where(fun i -> predicate i)

    member inline this.whereNot(predicate: 'Out -> bool) =
        this.WhereNot(fun i -> predicate i)

    member inline this.zipWith(other, combine: 'Out -> 'OtherOut -> 'NewOut) =
        this.ZipWith(other, fun i oi -> combine i oi)
