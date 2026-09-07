package uk.ac.ebi.utils.opt.runcontrol;

import static org.junit.Assert.assertEquals;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Test;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import uk.ac.ebi.utils.opt.runcontrol.ReactorUtils.ParallelBatchFluxBuilder;

/**
 *
 * @author Marco Brandizi
 * <dl><dt>Date:</dt><dd>29 Jun 2024</dd></dl>
 *
 */
public class ReactorUtilsTest
{
	@Test
	public void testParallelFlux ()
	{
		int max = 9;
		
		Stream<Integer> strm = IntStream.range ( 0, max )
		.mapToObj ( Integer::valueOf );
		
		
		ParallelFlux<Set<Integer>> flux = new ParallelBatchFluxBuilder<Integer, Set<Integer>> ( strm )
		.withBatchSize ( 3 )
		.withBatchSupplier ( HashSet::new )
		.build ();
	
		Set<Integer> maxes = flux.map ( 
			b -> b.stream ().max ( Comparator.naturalOrder () ).orElse ( 0 ) 
		)
		.sequential ()
		.collect ( Collectors.toSet () )
		.block ();
		
		assertEquals ( "Result is wrong!", Set.of ( 2, 5, 8 ), maxes );
	}
	
	@Test
	public void testBatchProcessing ()
	{
		int max = 1000;
		
		Stream<Integer> strm = IntStream.range ( 0, max )
		.mapToObj ( Integer::valueOf );
		
		
		AtomicInteger sum = new AtomicInteger ();
		
		ReactorUtils.batchProcessing ( 
			strm, b -> sum.addAndGet ( b.stream ().mapToInt ( Integer::intValue ).sum () )
		);
		
		// Usual Gauss formula for Sum (1..n)
		assertEquals ( "Result isn't as expected!", max * (max - 1) / 2, sum.get () );
	}
	
	
	@Test
	public void testPagedFlux ()
	{
		var pages = List.of ( "Hello", "Reactive", "World" );
		Function<String, Publisher<Character>> pageElementsProvider = 
			page -> Flux.fromStream ( page.chars ().mapToObj ( c -> (char) c ) );
			
		Flux<Character> flux = ReactorUtils.pagedFlux ( 
			Flux.fromStream ( pages.stream () ), pageElementsProvider
		);
		
		List<Character> chars = flux.collectList ().block ();
		List<Character> expectedChars = pages.stream ()
			.flatMap ( page -> page.chars ().mapToObj ( c -> (char) c ) )
			.collect ( Collectors.toList () );
		
		assertEquals ( "Result isn't as expected!", expectedChars, chars );
	}
	

	@Test
	public void testOffsetBasedPagedFlux ()
	{
		int npages = 10;
		int pgSize = 20;
		
		IntFunction<String> elementProvider = i -> "element " + i;
		
		Function<Long, Integer> pageProvider = ofs -> 
			ofs < npages * pgSize
				? Integer.valueOf ( ofs.intValue () )
				: null;
			
		Function<Integer, Publisher<String>> pageElementsProvider = 
			page -> Flux.fromStream ( IntStream.range ( page.intValue (), page.intValue () + pgSize )
				.mapToObj ( elementProvider ) );
			
		Flux<String> flux = ReactorUtils.pagedFlux (
			pageProvider, pgSize, pageElementsProvider 
		);
		
		List<String> result = flux.collectList ().block ();
		List<String> expected = IntStream.range ( 0, npages * pgSize )
			.mapToObj ( elementProvider )
			.collect ( Collectors.toList () );
		
		assertEquals ( "Result isn't as expected!", expected, result );
	}
	
	
	@Test
	public void testOffsetBasedPagedFluxWithPageProvider ()
	{
		int npages = 10;
		int pgSize = 20;
		
		Function<Long, Publisher<Integer>> pageProvider = ofs -> 
			ofs < npages * pgSize
				? Flux.fromStream ( IntStream.range ( ofs.intValue (), ofs.intValue () + pgSize )
					.mapToObj ( Integer::valueOf ) )
				: null;
		
		Flux<Integer> flux = ReactorUtils.pagedFlux ( pageProvider, pgSize );
		
		List<Integer> result = flux.collectList ().block ();
		
		List<Integer> expected = IntStream.range ( 0, npages * pgSize )
			.mapToObj ( Integer::valueOf )
			.collect ( Collectors.toList () );
		
		assertEquals ( "Result isn't as expected!", expected, result );
	}
}
