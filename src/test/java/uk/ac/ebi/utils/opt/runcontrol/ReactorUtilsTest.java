package uk.ac.ebi.utils.opt.runcontrol;

import static org.junit.Assert.assertEquals;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Test;

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
}
