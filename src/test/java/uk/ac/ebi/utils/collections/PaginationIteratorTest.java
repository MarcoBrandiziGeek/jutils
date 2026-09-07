package uk.ac.ebi.utils.collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import org.junit.Test;

/**
 * @author Marco Brandizi
 * <dl><dt>Date:</dt><dd>7 Oct 2024</dd></dl>
 *
 */
public class PaginationIteratorTest
{
	@Test
	public void testPaginationIterator ()
	{
		int npages = 10;
		int pgSize = 100;
		
		Iterator<Integer> pageItr = IntStream.range ( 0, npages ).iterator ();
		Function<Integer, Iterator<Integer>> pageElemProvider = 
			p -> IntStream.range ( p * pgSize, (p + 1) * pgSize  ).iterator ();
		
		var itr = new PaginationIterator<> ( pageItr, pageElemProvider );
		
		Integer i = 0;
		while ( itr.hasNext () )
			assertEquals ( "Wrong value returned by the pagination iterator!", i++, itr.next () );
	
		assertEquals ( "Wrong end value after the iteration", npages * pgSize, (int) i );
	}
	
	@Test
	public void testPaginationIteratorEmptyPager ()
	{
		Iterator<Integer> pageItr = new Iterator<>() 
		{
			@Override
			public Integer next ()
			{
				throw new UnsupportedOperationException ( "It's an empty iterator" );
			}
			
			@Override
			public boolean hasNext ()
			{
				return false;
			}
		};
		
		Function<Integer, Iterator<Integer>> pageElemProvider = 
			p -> IntStream.range ( p * 100, (p + 1) * 100  ).iterator ();

		var itr = new PaginationIterator<> ( pageItr, pageElemProvider );

		assertFalse ( "Iterator with empty pager has an element!", itr.hasNext () );
	}

	
	@Test
	public void testPaginationIteratorEmptyPage ()
	{
		Iterator<Integer> pageItr = IntStream.range ( 0, 10 ).iterator ();
		
		Iterator<Integer> emptyItr = new Iterator<>() 
		{
			@Override
			public Integer next ()
			{
				throw new UnsupportedOperationException ( "It's an empty iterator" );
			}
			
			@Override
			public boolean hasNext ()
			{
				return false;
			}
		};
		
		Function<Integer, Iterator<Integer>> pageElemProvider = 
			p -> emptyItr;

		var itr = new PaginationIterator<> ( pageItr, pageElemProvider );

		assertFalse ( "Iterator with empty page has an element!", itr.hasNext () );
	}
	
	
	@Test
	public void testOffsetBasedPageIterator ()
	{
		int npages = 10;
		int pgSize = 100;
		
		Function<Long, Integer> pgSelector = ofs -> ofs < npages * pgSize ? ofs.intValue () : null;		
		Function<Integer, Iterator<Integer>> pageElemProvider = 
			ofs -> IntStream.range ( ofs, ofs + pgSize  ).iterator ();
		
		var itr = new PaginationIterator<> ( pgSelector, pgSize, pageElemProvider );
		
		Integer i = 0;
		while ( itr.hasNext () )
			assertEquals ( "Wrong value returned by the pagination iterator (with page selector)!", i++, itr.next () );
	
		assertEquals ( "Wrong end value after the iteration (with page selector)", npages * pgSize, (int) i );
	}
	
	@Test
	public void testOffsetBasedElementsIterator ()
	{
		int npages = 10;
		int pgSize = 100;
		
		IntFunction<String> elemProvider = i -> "element " + i;
		
		Function<Long, Iterator<String>> pageQuery = ofs -> ofs < npages * pgSize
			? IntStream.range ( ofs.intValue (), ofs.intValue () + pgSize )
				.mapToObj ( elemProvider )
				.iterator ()
			: null;
				
		var itr = PaginationIterator.offsetBasedElementsIterator ( pageQuery, pgSize );
		
		Integer i = 0;
		while ( itr.hasNext () )
			assertEquals ( "Wrong value returned by the offsetBasedElementsIterator()!", 
				elemProvider.apply ( i++ ), itr.next () 
			);
			
		assertEquals ( 
			"Wrong end value after the offsetBasedElementsIterator() iteration", npages * pgSize, (int) i
		);
	}
		
}
