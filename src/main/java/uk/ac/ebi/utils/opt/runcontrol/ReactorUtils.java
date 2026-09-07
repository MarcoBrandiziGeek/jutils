package uk.ac.ebi.utils.opt.runcontrol;

import static reactor.core.scheduler.Schedulers.newBoundedElastic;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;
import uk.ac.ebi.utils.collections.PaginationIterator;

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
		
		/**
		 * This requires that you set a source later, via some {@code withSource()} method.
		 * Else, #build() will raise an error.
		 */
		public ParallelBatchFluxBuilder ()
		{
			// Nothing to do
		}

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

		@SuppressWarnings ( "unchecked" )
		public ParallelBatchFluxBuilder<T, B> withSource ( Flux<? extends T> flux )
		{
			this.flux = (Flux<T>) flux;
			return this;
		}
		
		public ParallelBatchFluxBuilder<T, B> withSource ( Stream<? extends T> stream )
		{
			return withSource ( Flux.fromStream ( stream ) );
		}
		
		public ParallelBatchFluxBuilder<T, B> withSource ( Collection<? extends T> collection )
		{
			return withSource ( collection.stream () );
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
		
		public ParallelFlux<B> build ()
		{
			if ( flux == null ) throw new IllegalStateException ( 
				"Can't build a parallel flux over a null source, use withSource() or a constructor"
			);
			
			@SuppressWarnings ( "unchecked" )
			Flux<B> result = this.batchSupplier == null 
				? (Flux<B>) flux.buffer ( batchSize ) : flux.buffer ( batchSize, batchSupplier );
			
			return result
			.parallel ( parallelism, parallelismPreFetch )
			.runOn ( scheduler );
		}		
	} // class ParallelBatchFluxBuilder
	
	
	/**
	 * Uses {@link ParallelBatchFluxBuilder} with its defaults.
	 */
	public static <T> ParallelFlux<List<T>> parallelBatchFlux ( Flux<? extends T> flux )
	{
		return new ParallelBatchFluxBuilder<T, List<T>> ( flux ).build ();
	}
	
	
	/**
	 * Uses {@link ParallelBatchFluxBuilder} with its defaults.
	 */
	public static <T> ParallelFlux<List<T>> parallelBatchFlux ( Stream<? extends T> stream ) 
	{
		return new ParallelBatchFluxBuilder<T, List<T>> ( stream ).build ();
	}
		
	/**
	 * Uses {@link ParallelBatchFluxBuilder} with its defaults.
	 */
	public static <T> ParallelFlux<List<T>> parallelBatchFlux ( Collection<? extends T> collection ) 
	{
		return new ParallelBatchFluxBuilder<T, List<T>> ( collection ).build ();
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

	
	/**
	 * Helper to emit a {@link Flux} from a paginated source of data.
	 * 
	 * TLDR: this is a reactive version of {@link PaginationIterator}, which is obtained by simply 
	 * combining {@link Flux#from(Publisher)} and {@link Flux#concatMap(Function)}.
	 * 
	 * Similarly to {@link PaginationIterator},
	 * this takes a publisher of pages and a function that yields a publisher of elements for each page, and then
	 * orchestrates the emission of elements from each page, until the page publisher is exhausted.
	 * 
	 * @param <P> the page type
	 * @param <E> the type of elements that {@code P} pages can provide
	 * @param pagePublisher a publisher of pages, which is expected to complete when there are no more pages to emit
	 * @param pageElementsProvider a function that yields a publisher of elements from a page. This publisher too is
	 * expected to complete when there are no more elements in the page.
	 * 
	 * @return a {@link Flux} of elements, which will alternate the switch to a new page, the emission of all the elements
	 * of the current page and the switch to the next page, until no more pages are available. 
	 * The resulting flux will complete when the page publisher is exhausted.
	 */
	public static <P, E> Flux<E>  pagedFlux ( 
		Publisher<? extends P> pagePublisher,
		Function<? super P, ? extends Publisher<? extends E>> pageElementsProvider
	)
	{
		return Flux.from ( pagePublisher )
			.flatMap ( page -> Flux.from ( pageElementsProvider.apply ( page ) ) );
	}
	
	/**
	 * A variant of {@link #pagedFlux(Publisher, Function)} 
	 * based on offset pagination.
	 * 
	 * This wraps the result of {@link PaginationIterator#offsetBasedPageIterator(Function, long)} 
	 * into a {@link Flux} and then calls {@link #pagedFlux(Publisher, Function)}.
	 * 
	 * TODO: <b>WARNING</b>: this is not fully reactive, since it uses an iterator to get the pages.
	 * To be implemented with {@link Flux#generate(Consumer)} or alike.
	 * 
	 */
	public static <P, E> Flux<E> pagedFlux (
		Function<Long, ? extends P> nextPageSelector,
		long pageSize,
		Function<? super P, ? extends Publisher<? extends E>> pageElementsProvider
	)
	{
		Flux<P> pagePublisher = Flux.fromIterable ( 
			() -> PaginationIterator.offsetBasedPageIterator ( nextPageSelector, pageSize ) 
		);
				
		return pagedFlux ( pagePublisher, pageElementsProvider );
	}
	
	
	/**
	 * A variant of {@link #pagedFlux(Function, long, Function)} that uses the page itself 
	 * as the page selector.
	 * 
	 * This is useful in languages like SQL or paginated APIs, where the page selector is 
	 * a query over the current record window.
	 * 
	 * This is the reactive version of {@link PaginationIterator#offsetBasedElementsIterator(Function, long)}.
	 * 
	 */
	public static <P, E> Flux<E> pagedFlux (
		Function<Long, ? extends Publisher<? extends E>> pageElementsProvider, long pageSize
	)
	{
		return pagedFlux ( pageElementsProvider, pageSize, Function.identity () );
	}
}
