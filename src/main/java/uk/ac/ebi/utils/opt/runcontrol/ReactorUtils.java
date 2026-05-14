package uk.ac.ebi.utils.opt.runcontrol;

import static reactor.core.scheduler.Schedulers.newBoundedElastic;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

/**
 * Utilities based on the Project Reactor library.
 *
 * @author Marco Brandizi
 * <dl><dt>Date:</dt><dd>29 Jun 2024</dd></dl>
 *
 */
public class ReactorUtils
{
	/**
	 * Little helper to build a common {@link ParallelFlux} to process a source of items
	 * in parallel batches.
	 *
	 * @author Marco Brandizi
	 * <dl><dt>Date:</dt><dd>30 Jun 2024</dd></dl>
	 *
	 */
	public static class ParallelBatchFluxBuilder<T, B extends Collection<T>>
	{
		/**
		 * {@link Schedulers#newBoundedElastic(int, int, String)} with the number of 
		 * processors as thread cap and that number * 50 as queue max size.
		 *  
		 * This seems suitable for batch processing, where we don't have much thread
		 * switching and we enqueue a flood of tasks.
		 */
		public static final Scheduler DEFAULT_FLUX_SCHEDULER = newBoundedElastic (
			Runtime.getRuntime ().availableProcessors (),
			Runtime.getRuntime ().availableProcessors () * 100,				
			"jutils.batchSched" 
		);
		
		/**
		 * This has been tested in tasks like saving data on a database.
		 */
		public static final int DEFAULT_BATCH_SIZE = 2500;

		
		private Flux<T> flux;
		private int parallelism = Schedulers.DEFAULT_POOL_SIZE,
			parallelismPreFetch = Queues.SMALL_BUFFER_SIZE;
		private Scheduler scheduler = DEFAULT_FLUX_SCHEDULER;
		private int batchSize = DEFAULT_BATCH_SIZE;
		private Supplier<B> batchSupplier;
		
		@SuppressWarnings ( "unchecked" )
		public ParallelBatchFluxBuilder ( Flux<? extends T> flux )
		{
			this.flux = (Flux<T>) flux;
		}
		
		public ParallelBatchFluxBuilder ( Stream<? extends T> stream )
		{
			this ( Flux.fromStream ( stream ) );
		}

		public ParallelBatchFluxBuilder ( Collection<? extends T> collection )
		{
			this ( collection.stream () );
		}
				
		/**
		 * The degree of parallelism of the resulting flux. This is passed to 
		 * {@link Flux#parallel(int, int)}. Defaults to {@link Schedulers#DEFAULT_POOL_SIZE}, as
		 * per Reactor default.
		 */
		public ParallelBatchFluxBuilder<T, B> withParallelism ( int parallelism )
		{
			this.parallelism = parallelism;
			return this;
		}
		
		/**
		 * The prefetch parameter passed to {@link Flux#parallel(int, int)}. Default is 
		 * {@link Queues#SMALL_BUFFER_SIZE}, as per Reactor default.
		 */
		public ParallelBatchFluxBuilder<T, B> withParallelismPreFetch ( int parallelismPreFetch )
		{
			this.parallelismPreFetch = parallelismPreFetch;
			return this;
		}
		
		/**
		 * The scheduler used to run the resulting flux. This is passed to 
		 * {@link ParallelFlux#runOn(Scheduler)}. 
		 * 
		 * Default is {@link #DEFAULT_FLUX_SCHEDULER}, as per Reactor default.
		 */
		public ParallelBatchFluxBuilder<T, B> withScheduler ( Scheduler scheduler )
		{
			this.scheduler = scheduler;
			return this;
		}
		
		/**
		 * The parallel flux scheduler to use. This is passed to {@link ParallelFlux#runOn(Scheduler)}.
		 * Defaults it {@link #DEFAULT_BATCH_SIZE}, as per Reactor default.
		 */
		public ParallelBatchFluxBuilder<T, B> withBatchSize ( int batchSize )
		{
			this.batchSize = batchSize;
			return this;
		}

		/**
		 * Default is null, which falls back to {@link Flux#buffer(int)}, usually a {@link List} supplier.
		 */
		@SuppressWarnings ( "unchecked" )
		public ParallelBatchFluxBuilder<T, B> withBatchSupplier ( Supplier<? extends Collection<? super T>> batchSupplier )
		{
			this.batchSupplier = (Supplier<B>) batchSupplier;
			return this;
		}
		
		
		public int getParallelism ()
		{
			return parallelism;
		}

		public void setParallelism ( int parallelism )
		{
			this.parallelism = parallelism;
		}

		public int getParallelismPreFetch ()
		{
			return parallelismPreFetch;
		}

		public Scheduler getScheduler ()
		{
			return scheduler;
		}

		public int getBatchSize ()
		{
			return batchSize;
		}

		public Supplier<B> getBatchSupplier ()
		{
			return batchSupplier;
		}
		
		/**
		 * @param visitor if non-null, I'll call it with myself before creating the result. This can be
		 * used to inspect a builder during the build process, eg, to get info like 
		 * {@link ParallelBatchFluxBuilder#getBatchSize() the default batch size}, to be used later by
		 * your invoker.
		 *
		 * TODO: Review, it was used once in KnetMiner API, but not sure it's really useful.
		 */
		public ParallelFlux<B> build ( Consumer<ParallelBatchFluxBuilder<? super T, ? super B>> visitor )
		{
			@SuppressWarnings ( "unchecked" )
			Flux<B> result = this.batchSupplier == null 
				? (Flux<B>) flux.buffer ( batchSize ) : flux.buffer ( batchSize, batchSupplier );

			if ( visitor != null ) visitor.accept ( this );
			
			return result
			.parallel ( parallelism, parallelismPreFetch )
			.runOn ( scheduler );
		}
		
		public ParallelFlux<B> build ()
		{
			return build ( null );
		}
		
	} // class ParallelBatchFluxBuilder
	
	
	/**
	 * Uses {@link ParallelBatchFluxBuilder} with its defaults, then pass it the visitor as in
	 * {@link #build(Consumer)} and finally builds a result that batches the source flux.
	 */
	public static <T> ParallelFlux<List<T>> parallelBatchFlux (
		Flux<? extends T> flux, Consumer<ParallelBatchFluxBuilder<? super T, ? super List<T>>> visitor
	)
	{
		return new ParallelBatchFluxBuilder<T, List<T>> ( flux )
		.build ( visitor );
	}

	/**
	 *	Batches the flux with the default {@link ParallelBatchFluxBuilder}. 
	 */
	public static <T> ParallelFlux<List<T>> parallelBatchFlux ( Flux<? extends T> flux ) {
		return parallelBatchFlux ( flux, null );
	}
	
	
	/**
	 * Batches the stream with a {@link ParallelBatchFluxBuilder parallel flux builder} that was
	 * tweaked using {@link ParallelBatchFluxBuilder#build(Consumer) a builder visitor}. 
	 */
	public static <T> ParallelFlux<List<T>> parallelBatchFlux (
		Stream<? extends T> stream, Consumer<ParallelBatchFluxBuilder<? super T, ? super List<T>>> visitor
	) 
	{
		return new ParallelBatchFluxBuilder<T, List<T>> ( stream ).build ( visitor );
	}

	public static <T> ParallelFlux<List<T>> parallelBatchFlux ( Stream<? extends T> stream  ) {
		return parallelBatchFlux ( stream, null );
	}
	
	
	/**
	 * Creates a default {@link ParallelBatchFluxBuilder}, pass the visitor to it via 
	 * {@link ParallelBatchFluxBuilder#build(Consumer)} and returns the built parallel flux. 
	 */
	public static <T> ParallelFlux<List<T>> parallelBatchFlux (
		Collection<? extends T> collection, Consumer<ParallelBatchFluxBuilder<? super T, ? super List<T>>> visitor
	) 
	{
		return new ParallelBatchFluxBuilder<T, List<T>> ( collection ).build ( visitor );
	}
	
	/**
	 * Batches the collection with the default {@link ParallelBatchFluxBuilder}. 
	 */
	public static <T> ParallelFlux<List<T>> parallelBatchFlux ( Collection<? extends T> collection  ) {
		return parallelBatchFlux ( collection, null );
	}
	
	/**
	 * Just uses the Reactor methods to make the parallel flux of batches process each batch
	 * with the given task (ie, pass it to {@link ParallelFlux#doOnNext(Consumer)}) and 
	 * then wait for that to finish (ie, calls {@link ParallelFlux#sequential()} and then
	 * {@link Flux#blockLast()}. Returns 
	 */
	public static <T, B extends Collection<? super T>> void batchProcessing (
	  ParallelFlux<B> parallelFlux, Consumer<B> task		
	)
	{
		parallelFlux.doOnNext ( task )
		.sequential ()
		.blockLast ();
	}

	/**
	 * Uses {@link ParallelBatchFluxBuilder} with default options and
	 * {@link #batchProcessing(ParallelFlux, Consumer)} to batch a source of items and 
	 * process them in parallel batches.
	 *   
	 */
	public static <T> void batchProcessing ( Flux<T> flux, Consumer<List<T>> task	)
	{
		batchProcessing ( parallelBatchFlux ( flux ), task );
	}

	/**
	 * Variant of {@link #batchProcessing(Flux, Consumer)}
	 */
	public static <T> void batchProcessing ( Stream<T> stream, Consumer<List<T>> task	)
	{
		batchProcessing ( parallelBatchFlux ( stream ), task );
	}

	/**
	 * Variant of {@link #batchProcessing(Flux, Consumer)}
	 */
	public static <T> void batchProcessing ( Collection<T> collection, Consumer<List<T>> task	)
	{
		batchProcessing ( parallelBatchFlux ( collection ), task );
	}
	
}
