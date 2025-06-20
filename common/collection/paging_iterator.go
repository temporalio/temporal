package collection

type (
	// PaginationFn is the function which get a page of results
	PaginationFn[V any] func(paginationToken []byte) ([]V, []byte, error)

	// PagingIteratorImpl is the implementation of PagingIterator
	PagingIteratorImpl[V any] struct {
		paginationFn      PaginationFn[V]
		pageToken         []byte
		pageErr           error
		pageItems         []V
		nextPageItemIndex int
	}
)

// NewPagingIterator create a new paging iterator
func NewPagingIterator[V any](
	paginationFn PaginationFn[V],
) Iterator[V] {
	iter := &PagingIteratorImpl[V]{
		paginationFn:      paginationFn,
		pageToken:         nil,
		pageErr:           nil,
		pageItems:         nil,
		nextPageItemIndex: 0,
	}
	iter.getNextPage() // this will initialize the paging iterator
	return iter
}

// NewPagingIteratorWithToken create a new paging iterator with initial token
func NewPagingIteratorWithToken[V any](
	paginationFn PaginationFn[V],
	pageToken []byte,
) Iterator[V] {
	iter := &PagingIteratorImpl[V]{
		paginationFn:      paginationFn,
		pageToken:         pageToken,
		pageErr:           nil,
		pageItems:         nil,
		nextPageItemIndex: 0,
	}
	iter.getNextPage() // this will initialize the paging iterator
	return iter
}

// HasNext return whether has next item or err
func (iter *PagingIteratorImpl[V]) HasNext() bool {
	// pagination encounters error
	if iter.pageErr != nil {
		return true
	}

	// still have local cached item to return
	if iter.nextPageItemIndex < len(iter.pageItems) {
		return true
	}

	if len(iter.pageToken) != 0 {
		iter.getNextPage()
		return iter.HasNext()
	}

	return false
}

// Next return next item or err
func (iter *PagingIteratorImpl[V]) Next() (V, error) {
	if !iter.HasNext() {
		panic("HistoryEventIterator Next() called without checking HasNext()")
	}

	if iter.pageErr != nil {
		err := iter.pageErr
		iter.pageErr = nil
		var v V
		return v, err
	}

	// we have cached events
	if iter.nextPageItemIndex < len(iter.pageItems) {
		index := iter.nextPageItemIndex
		iter.nextPageItemIndex++
		return iter.pageItems[index], nil
	}

	panic("HistoryEventIterator Next() should return either a history event or a err")
}

func (iter *PagingIteratorImpl[V]) getNextPage() {
	items, token, err := iter.paginationFn(iter.pageToken)
	if err == nil {
		iter.pageItems = items
		iter.pageToken = token
		iter.pageErr = nil
	} else {
		iter.pageItems = nil
		iter.pageToken = nil
		iter.pageErr = err
	}
	iter.nextPageItemIndex = 0
}
