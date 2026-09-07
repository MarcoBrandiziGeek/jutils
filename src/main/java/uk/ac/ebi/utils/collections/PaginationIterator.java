package uk.ac.ebi.utils.collections;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * A generic helper to build a pagination iteration.
 * 
 * The idea is that a page iterator offers a new page {@code P} every time it is
 * asked and a page elements function yields an iterator over the elements of
 * a page. This very abstract model can be applied to a big number of sources
 * that need to be queried with pagination, eg, databases or APIs.
 *
 * @param <P> the page the iterator deals with. Examples: a number, an API
 * bookmark value, anything that allows a page element provider to produce an element
 * iterator from a page. In particular, this could be the page element iterator itself,
 * with the page elements provider being the identity function 
 * (see {@link #offsetBasedPageIterator(Function, long)}).
 *
 * @param <E> the elements that the iterator returns by switching from one page to the next.
 * The page elements provider in the constructors yield an iterator that iterates over the elements
 * of a (current) page.
 *
 * @author Marco Brandizi
 * <dl><dt>Date:</dt><dd>7 Oct 2024</dd></dl>
 *
 */
public class PaginationIterator<P, E> implements Iterator<E>
{
	private Iterator<? extends P> pageIterator;
	private Function<? super P, ? extends Iterator<? extends E>> pageElementsProvider;

	private Iterator<? extends E> currentPageIterator = null;
	private boolean isFinished = false;

	/**
	 * Builds a page iterator for {@link PaginationIterator} that moves from one page
	 * to the next based on pageSize and returns a page based on the selector function.
	 * 
	 * @param nextPageSelector yields a page, from an offset parameter, ie, an element index
	 * that says where we are in the data source. Eg, the value passed to SKIP or OFFSET
	 * in an SQL query. Since this is based on 
	 * {@link IteratorUtils#supplier2Iterator(java.util.function.Supplier)}, this function
	 * must return null when (and only when) there aren't further pages to iterate through.
	 * Note that the result of this could be the same as the page elements iterator, eg, 
	 * you might have a paged SQL (or API) that issues a query with the current page and returns
	 * the database cursor/result-set if the hasNext() method of the latter is true, else it
	 * returns null.  
	 * 
	 * @param pageSize the resulting iterator increases an internal offset by this amount
	 * every time a new page P has to be produced. The page selector receives this offset
	 * when needed to produce the page.
	 * 
	 */
	public static <P> Iterator<P> offsetBasedPageIterator (
		Function<Long, ? extends P> nextPageSelector, long pageSize 
	)
	{
		long [] offset = new long[] { 0 };
		return IteratorUtils.supplier2Iterator 
		( 
			() -> { 
				var result = nextPageSelector.apply ( offset [ 0 ] );
				offset [ 0 ] += pageSize;
				return result;
			}
		);
	}
	
	/**
	 * Builds an offset-based paginated iterator, where the page selector returns the 
	 * page element iterator straight.
	 * 
	 * This use case is typical of languages like SQL or paginated APIs, where the page selector
	 * is a query over the current record window.
	 * 
	 */	
	public static <E> PaginationIterator<Iterator<? extends E>, E> offsetBasedElementsIterator (
	  Function<Long, ? extends Iterator<? extends E>> pageElementsProvider, long pageSize
	)
	{
		return new PaginationIterator<> ( pageElementsProvider, pageSize, Function.identity () );
	}
	
	
	/**
	 * @param pageIterator as said above, this provides a new page every time that's needed.
	 * 
	 * @param pageElementsProvider as said above, this provides an iterator over the elements
	 * of the parameter page. The pagination iterator works by asking a new page, asking the page
	 * elements iterator, iterating on the latter until exhaustion and going back to ask the next
	 * page as more elements are requested, terminating when the page iterator is exhausted too.
	 * 
	 */
	public PaginationIterator ( 
		Iterator<? extends P> pageIterator,
		Function<? super P, ? extends Iterator<? extends E>> pageElementsProvider
	)
	{
		super ();
		this.pageIterator = pageIterator;
		this.pageElementsProvider = pageElementsProvider;
	}

	/**
	 * Builds the page iterator as offset-based, 
	 * using {@link #offsetBasedPageIterator(Function, long)}.
	 */
	public PaginationIterator (
		Function<Long, ? extends P> nextPageSelector, long pageSize,
		Function<? super P, ? extends Iterator<? extends E>> pageElementsProvider
	)
	{
		this ( offsetBasedPageIterator ( nextPageSelector, pageSize ), pageElementsProvider );
	}
	
	
	/**
	 * See {@link #PaginationIterator(Iterator, Function)} for details on how we loop.
	 */
	@Override
	public boolean hasNext ()
	{
		if ( isFinished ) return false;
		
		if ( currentPageIterator != null && currentPageIterator.hasNext () )
			return true;
				
		if ( !pageIterator.hasNext () ) {
			isFinished = true;
			return false;
		}
		
		P nextPage = pageIterator.next ();
		currentPageIterator = pageElementsProvider.apply ( nextPage );
		
		return currentPageIterator.hasNext ();
	}

	/**
	 * See {@link #PaginationIterator(Iterator, Function)} for details on how we loop.
	 */
	@Override
	public E next ()
	{
		if ( !hasNext () ) throw new NoSuchElementException (
			"PaginationIterator, no more elements available"	
		);
		return currentPageIterator.next ();
	}

}
