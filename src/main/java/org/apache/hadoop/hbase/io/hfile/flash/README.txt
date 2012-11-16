HBASE cache changes
===================

1. Rationale
~~~~~~~~~~~~
Many HBase clusters end up growing in size for performance reasons. To get good
IO performance, especially with disc drives, the working set must fit in RAM.
It's not feasible to put terabytes of RAM per server; similarly, it's not
feasible in typical installations to place all data on NAND flash. Instead,
if we can use Flash to augment the in-memory page cache, in a persistent
manner, we can allow a hybrid of Flash and disc in each region server without
affecting the resiliency of the cluster. Furthermore, placing the Flash close
to the core of HBase means it won't be accessed through the extra layers of
HDFS code so should offer better perforance.

2. Design approach
~~~~~~~~~~~~~~~~~~
The design centre is around several terabytes of Flash per server. Although
this may be an excessive quantity today, we design for the future. The
design goals are:
  a) Act as a victim cache for a _small_ in-memory LRUBlockCache.
  b) Fail gracefully in the event of drive failure.
  c) Don't block on cache inserts; do them in the background.
  d) Don't contend for locks on reads
  e) Don't use up large space or large object counts on the heap.
  f) Provide efficient eviction taking into account recency as well as frequency
  g) Use the same code to implement a cache on block devices as well as off the heap

The interfaces added are also sufficiently generic to enable cache chaining;
so you can have an on-heap LRUBlockCache whose victims fall into an off-heap
cache; in turn, whose victims can fall into a flash cache.

So although this cache is called FlashCache, it provides an efficient off-heap cache
too, using the same code base. Work in CacheConfig hasn't been done to actually
enable cache chaining, however almost all the interfaces are in place. Until we've
done some real-world benchmarking, the merits of this configuraiton vs just putting
everything in Flash can't be assessed.

3. Configuration
~~~~~~~~~~~~~~~~
Configuration is made with the following parameters in hbase-site.xml:

  hbase.flashcache.filesize: Integer, in megabytes.
      This is the size of the cache in megabytes. It apples to both an off-heap
      cache as well as a cache on Flash. If using off-heap memory, the JVM must
      be configured with the correct command line options to allow allocation to
      succeed.

  hbase.flashcache.writerThreads: Integer
      Data is asynchronously flushed to the underlying cache (on flash or off heap)
      so that we don't block calling threads unnecesary. Since Java 1.6 doesn't have
      asynchronous non-blocking IO, we do blocking IO with multiple threads instead.
      A minimum of 4 threads, a maximum of 16, is what we've tested with. The exact
      number depends on the performance of the underlying drive. Applies to off-heap and
      flash caches.

  hbase.flashcache.writerQueueLength: Integer
      Number of items we can accumulate for pushing to the writer threads. Suggest
      a minimum of 64. If the writer threads are slow, and the queue backs up,
      insertions into the cache will be discarded rather than blocking the caller.
      Applies to off-heap and flash caches.

  hbase.flashcache.metadataname: String path to filename
      File name to which cache metadata is written on shutdown. Metadata includes the
      dictionary of what's stored in the cache, as well as eviction usage statistics.
      Applies to flash-backed caches only, since the heap is non-persistent.

  hbase.flashcache.filename: Prefixed path etc.
      This string can be off one of the following formats:
      	    (a) file:/caches/cachefile.dat
            (b) directHash:file:/caches/cachefile.dat
            (c) offHeap:/whatever
            (d) directHash:offHeap:/whateverElse
      For (a) and (b) the actual path name is the path to the file used to back the
      persistent cache on the drive. For (c) and (d) an off-heap cache is used instead,
      so the path doesn't matter. For (a) and (c), the directHash: prefix means an
      off-heap hash table implementation is used instead of the usual Java concurrent
      hash-map. This has maybe a 3% execution overhead however it avoids GC of any
      key or value objects, as well as reduces the amount of memory occupied by them.

      There is in fact another locator that can be used instead of offHeap: - heap: -
      in which case the backing arrays for the off-heap cache are allocated on the Java
      heap instead. This seemed to have awful performance at GC time, so if you want to
      do this, better just use the built-in LRUBlockCache instead! This was a bad idea :)

For errors, look in the log. The log prints need to be tidied up a bit, but generally
allocation errors, file I/O errors and whatnot will be shown there. In the event of a
successful running cache, you should see regular prints of "Clock counted x reference
bits since last sweep" or the like. If you don't see those, look for exceptions or
errors earlier.

3. Key classes
~~~~~~~~~~~~~~
  DirectArray
    This class implements an array of fixed size structures, stored in byte buffers either on
    the Java heap or off it (direct). All it does is read and write arrays of bytes, one item
    or multiple items at a time. It solves a couple of problems inherent in just using one
    ByteBuffer:
      1- The ByteBuffer has a size limited by a 32-bit number.
      2- The ByteBuffer's access methods are not thread-safe, so no concurrency is possible.
    We solve both of these by allocating many smaller byte buffers, each of which is
    independently locked. There are fast path methods available to perform unlocked access
    when it's known to be safe, as well as methods to persiste and retrieve the contents of
    the array from Flash.
    DirectArray is used to implement both the off-heap cache (as an array of 1-byte items)
    as well as the off-heap hash table (as an array of key+value-sized items)

  ForwardMap
    ForwardMap is an interface used to map from cache keys to offsets of cache items within
    the cache. With an incoming key, we can look it up in the forward map to find the
    corresponding item offset in the cache. The forward map's keys are BlockCacheKeys and
    values are FlashCache.Entries.

    A very efficient iteration interface is presented that allows iteration of the whole
    map, and access to values within it, without creating any garbage. The caller creates
    the iterator by passing in a reference to an already-existing FlashCache.Entry that's
    re-used to return values during iteration. An opaque collection object,
    ForwardMap.ReferenceList can be used to save references to iterated items which can
    then be re-iterated - all without creating any garbage objects whatsoever.

    Implementing the standard Java iterator interface would be possible and easy to do,
    however it's benchmarked far less efficiently for iterating large collections fast.
    Since the only time we block _anything_ in the cache is while we work out what to
    evict, we need this to be as fast as possible, even for huge caches.

  DirectHashForwardMap:
    An implementation of ForwardMap using DirectHash. The BlockCacheKey references aren't
    held onto; the keys are serialised into bytes stored in the DirectHash underlying array.

  HeapForwardMap:
    A trivial implementation of ForwardMap using ConcurrentHashMap with BlockCacheKey. This
    holds onto references to BlockCacheKey as the keys for the map, but encodes the values
    in a very compact binary form (HeapForwardMap.CompactEntry) to minimise RAM usage.

  IOEngine:
    IOEngine is an interface implemented by an object that gets things to/from the underlying
    backing store. It's interface just looks like standard POSIX read/write.

  FlashIOEngine:
    An implementation of IOEngine that reads/writes with an underlying FileChannel. Used for
    persistent Flash-based caches.

  DirectArrayIOEngine:
    Implements cache read/writes using the DirectArray class. Used for off-heap caches.

  UniqueIndexMap:
    This is a class used to minimise and re-use a set of input values. For example, HBase
    may read and write many different block sizes, however there are a small number of uniquely
    used block sizes. Rather than storing the byte-length of the blocks in the meta-data,
    this class is used to map these to a simple number instead (0, 1, 2). In essence, a
    simple, persistent dictionary. It's currently used for minimising meta-data size for
    block lengths as well as references to deserialisers.

  FlashAllocator:
    The flash allocator manages underying space allocation, whether for the off-heap array
    or the file on disc. It implements a slab allocation scheme; bucket sizes in the
    allocator are powers of two plus a bit more to allow for extra meta-data along with the
    block (otherwise all 64K blocks, which actually take about 65K to serialise, would end
    up being stored in 128K buckets leading to big wastage!) To make life simple, based on
    available space, we allocate one bucket for each block size available up to a max(4MB).
    Each bucket is currently 16MB in size, so we end up with lots of buckets. Remaining
    buckets are all allocated at the largest item size (4MB).

    Allocation and freeing of items are both as-near-as-makes-no-difference O(1) and wastage
    is minimal due to tuning.

    When allocating, we try to find a bucket of the same size with free items; if we find
    one, we return it; if not, we downsize a completely empty larger bucket and re-allocate
    it. The general idea is that representative block-sized IOs will be done pretty quickly,
    so we can efficiently optmise layout for this.

    If another scheme was needed, we could do a sequential log-based allocation mechanism
    instead.

    A method is provided to map an offset back to the allocated size, so that the eviction
    and reclamation process can be object-size-aware. This is also O(1).

  FlashCache:
    This is the cache implementation that binds all of the above together. It implements the
    HBase cache interface by using the forward map, flash allocator and underlying IO engine.
    The asynchronous queued inserts, heat-map eviction logic and cache persistence are
    also implemented here.

    The insert and retrieve paths are completely contention free exception during eviction.
    There are several inner classes, as follows:

    FlashCache.Entry:
      This is a key to an item in the forward map. It stores the offset, length of the stored
      cache block, as well as a reference to an object that can be used to deserialise it.
      The length and deserialiser are encoded using UniqueIndexMap<T> dictionaries.

    FlashCache.RAMQueueEntry:
      When an entry is inserted into the cache, we immediately place it into a queue to
      be picked up by a writer thread. The queues are concurrent queues of RAMQueueEntry.
      These objects simply remember the cache key and not-yet-serialised data. They are
      also inserted into the FlashCache.mRAMCache map, so that cache gets can also return
      data from the write queues, even if they aren't written yet.

    FlashCache.ClockThread:
      This thread periodically ages references stored in the reference map. It's used for
      more efficient eviction; see section 7 (Eviction path) for more information.

    FlashCache.WriterThread:
      As its name suggests, multiple of these threads are created. Each thread has one
      associated inbound queue, to avoid contention. Each thread drains its queue in
      one shot and then writes the drained items to the store (off-heap or flash) using
      the IOEngine object.

    Remaining minor work: I still have to implement the thread to print out cache
    statistics periodically. One of those grunt jobs that hasn't been done yet. Likewise,
    implementing HBase's heap size stuff for allowing it to track heap size.

    Enhancement ideas: When things are evicted from the cache, we could TRIM them from
    the underlying device. Similarly, we could find a memory-efficient way to implement
    the evictBlocksByHfileName entry point that's used to throw away old data when segments
    have been merged. I'd sooner create a first class object for storing file names and
    cache keys in a numeric way before doing this, for efficiency reasons.

4. Other class changes
~~~~~~~~~~~~~~~~~~~~~~
  BlockCacheKey: This is the HBase key to something stored in the Flash. The off-heap
    hash table requires fixed-sized keys for its backing arrays. So I added code to lazily
    store a SHA-256 hash of the key when needed - this allows all BlockCacheKeys to be stored
    in 32 bytes. A better approach, since the file name and block number are already numeric,
    would be to just store these instead of the string value, at which point something much
    more CPU efficient could be done. However this would cascade through more of HBase - an
    easy change to make locally, but a lot of diffs to keep open ;) Serialisation was modified
    so that the extra field that stores the hash is not unnecessaily serialised.

  LRUBlockCache.java: This was changed to support 'cache chaining' such that victims - blocks
    evicted from it - are inserted into the next cache in sequence.

  HFileBlock: This was bug-fixed. Serialisation was broken after the new HFile format was added;
    the change here fixes serialisation so it works correctly. A JIRA is currently filed against
    HBase's current off heap cache that's actually caused by this underlying problem instead.
    There's also some debug methods still littered in here that can be removed.

  CacheableDeserializer:
    This class implements a reference to a deserializer. We can't keep object-references off
    heap (for DirectHash) and, in any case, an object reference uses a lot of bytes for what's
    essentially the same thing (today all file blocks actually use the same deserialiser). So
    I added a method to return an integer instead that can be used to dehydrate the deserialiser,
    and a new class CacheableDeserialiserFactory that creates CacheableDeserialisers from
    the saved integer.

  CacheableDeserializerFactory:
    This class re-creates CacheableDeserializers from a stored numeric deserializer ID.

5. Write (put) path
~~~~~~~~~~~~~~~~~~~
Cache inserts take place when items are evicted from the LRUBlockCache, through the cacheBlock
entry point.

The first thing we do is check if the block is already in the cache. If it is, we avoid a
double-insert. Since we never read the underlying block here - we're only deciding if we
really need to insert or not - we just do a check without grabbing any locks. If it's already
there, we're done and we bail.

If not, we get a FlashCache.RAMQueueEntry to hold a reference to the key/value pair, and put
it into one of the writer queues. We select writer queue by looking at the hash code of the
key modulo the number of queues we have, so inserts will be distributed fairly across all
queues/threads.

We place a reference to the FlashCache.RAMQueueEntry into our RAMCache dictionary, and then
place the FlashCache.RAMQueueEntry into the allocated queue; if the queue's full (because
the writer thread is backing up or something else has gone wrong) we log the Info, remove
the item from the queue and dictionary.

In both cases, we then return from the O(1) non-blocking code path.

6. Read (get) path
~~~~~~~~~~~~~~~~~~
The read path is also quite straightforward. Firstly, we check for a hit in the RAMCache
dictionary; this contains only the small number of recent items sitting in writer queues
waiting to be written to the backing store. If we find a hit there, we simply return and
we're done. In most cases, this will miss.

We then grab the reader/writer lock in read mode.

After grabbing the read lock, which allows multiple readers, we check in the forward map.
If the entry exists, we tell the IO engine to do the I/O into a local buffer (by copying
onto the heap or doing file IO), deserialise the contents and return the data, updating
the corresponding referenced bit in the first place.

If any IO errors occur, we assume something's broken and disable the cache. When the
cache is disabled, we don't honour any gets, we stop evictions, and we ignore all inserts.

7. WriterThread path (lazy insertion)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The writer threads are of class WriterThread. Their purpose is to drain cache items from
the inbound queue, and write them to the underlying media, to enable non-blocking inserts.
Each WriterThread is associated with one inbound queue.

For each iteration of the thread, we remove all items in the inbound queue so we can
deal with them in one shot (this requires one take() and one drainTo() call on the
inbound queue, since Java doesn't have an atomic block-but-then-get-everything call).
The thread runs an infinite loop doing this, since the take() blocks, exiting only
when interrupted at a consistent point during shutdown.

Once we've got the list of RAMQueueEntries that we need to write, we loop over
them one at a time, writing each one to cache. Writing each one involves serialising
them, calling the FlashAllocator instance to reserve space, then doing the underlying
write to storage through the IOEngine.

In the event of an allocation failing, we foreceably invoke the eviction path - on
this thread - and try again. The eviction path is fairly smart, since it knows if
one thread is blocking on eviction, chances are many more are also; subsequent threads
will take a fast-path out of eviction if a predecessor has already done it :)

We keep a list of those things successfully written, and - at the end of writing
all of them - fsync the underlying storage, to make sure they are persisted. This is
because, if we shut down and preserve metadata immediately, we need to make sure that
the blocks are written.

After syncing the underlying storage, we grab the writer lock, add the entries to the
forward map and then release the lock - as fast as possible, in a small tight loop.
Then, after releasing the lock, we remove the successfully written entries from the
transient RAMCache.

8. Eviction path
~~~~~~~~~~~~~~~~
Eviction is probably the most complex code paths in the cache. I wanted to come up
with a model that:
  o Makes sure that neither frequently used, not recently used, data is evicted at
    the expense of unreferenced stuff, which requires holding usage history
  o Adds zero contention on the read path, and at the same time adds minimal
    overhead for any background processes.
  o Doesn't add substantial meta-data overhead

If I chose an LRU or ARC type of cache, the linked lists for maintaining these would
become contention points. As simple clock could be used, but then frequent, but not
recent, data would get evicted unnecessarily.

So I chose to keep cache frequency and recency information out of the main forward
map and other data structures - so they can be updated without contention and
worrying about eviction and locking and whatnot - in a way that things are packed
together, so that doing any sweeping modifications is cache-locality-efficient too.

I chose a 'heat map' based approach; rather than keeping one referenced bit per
entry, we keep one byte. When an object in the forward map is referenced, we set
the top bit of the byte. Periodically, a background clock runs that walks all said
bytes, and shifts them right by 1 bit. So we get some historic referenced data too.
The clock thread grabs a lock, just so that it doesn't race access to these bytes
unnecessarily with the evictor - again, that wouldn't be a big deal, but it's just
cleaner this way.

The problem is, how do we index the array of bytes, since all we have is a key?
The approach I took was to keep one byte for each potential entry in the underlying
forward map's hash table, and also index this byte array by hashing the incoming
keys modulo the size of the array. In this case, collisions simply don't matter
since it's not critical data anyway (the worst that can happen is the odd block
might not get evicted when it should have been, or vice-versa). This array of
bytes is the CacheReferenced array, and it's kept on-heap.

We evict things from the cache only when the cache fills up. Perhaps we could
also do so periodically anyway, to keep things trim, but I've not measured such
gain. So today the free space eviction will be run on one of the writer threads.

The free space eviction process looks like this. It needs to be aware of the
bucketed nature of the allocator by understanding object size rounding.
I set a free space goal of 12.5% per object size. If any object size has less
than 12.5% free space remaining, the code will actually evict stuff --- and if
it evicts, it will try to trim all sizes to about 18% free. I just chose these
numbers, they are certainly meddlable, but seem to be working OK.

So the eviction process looks like this. We get the writer lock.
We stop the clock updater thread (or
wait a tiny amount of time for it) by grabbing the ClockLock. We then walk
the whole forward map, using the efficient iterators, and look up the
corresponding heat - shifting it right by one in the process, such that if we
evict more often than the clock thread is running, we'll have more current
recency information. We get these objects and add them to a priority-bucketed
list i.e. one list for each priority for each bucket size.

We could be smart here, and once we've got enough "low priority" stuff to
toss out, actually stop iterating the forward map. For really large caches,
this could bring some CPU gain - being careful to start next time where we
left off. Not implemented yet, though.

We run with the writer lock acquired because we need to make sure that the
Read (get) path doesn't end up finding something in Flash, and by the time
(or during) IO, it's been evicted and something else been put in its place!

After sorting, we iterate for each size group, tossing away items, in
ascending heat order, until we've met the free goal for that size group.
In each case, we call the flash allocator to free the corresponding block
too, and reset the referenced information in the heat map to 0.

After doing this, we release the writer lock as fast as possible. As
discussed earlier, an optimisation here could be to TRIM the underlying
storage at the same time we're removing things from the allocator, although
doing so would involve a lot of kernel calls with the writer lock held
(to avoid races of new data being written before trimmed) unless there's some
sort of batch multi-trim command available.

In debug mode, we then re-run the totalling up of all data stored in the
Fash allocator, and make sure that bottom-up our free space goals have been
met!

9. Notes on assertions
~~~~~~~~~~~~~~~~~~~~~~
There are lots of assertions and invariant test methods in the code. You'll
need to run with the appropriate JVM flag (-ea AFAIR) to enable these tests,
which will slow the system down a lot but are advisable when making any
code changes.

The converse of this is that, if you disable assertions, it will run
much faster :-)

10. Benchmarks
~~~~~~~~~~~~~~
Some testing has been done to compare the raw performance of the Flash cache
with the internal LRUBlockCache.

The first set of testing was performed with LRUBlockCache completely disabled,
and FlashCache as the only cache in the system. Compared to LRUBlockCache, we
get 30%-50% of the throughput of LRUBlockCache when all data fits in DRAM.
The reason for this is not the LRUBlockCache performance, per se -- in both
situations, the system is still CPU-bound. The problem is garbage. Each time
LRUBlockCache fetches a block, it just returns a buffer already on the heap.
For FlashCache, a new heap buffer has to be allocated, which is then thrown
away sometime later. This garbage - often large 64K buffers - forces much more
GC. Thus the system still ends up at 100% CPU, not IO-bound, but proortionally
more CPU is used GCing with FlashCache than with LRUBlockCache, hence the
lower throughput. This could be fixed by implementing buffer management in
HBase but doing so is a major code change.

Despite this, the results aren't bad: On my 4-core (hyperthreaded to 8) system,
LRUBlockCache with all data in DRAM gets around 30,000 YCSB ops/sec, the
equivalent with FlashCache is around 8,000-16,000 depending on block size.

In reality, LRUBlockCache will be present too. In doing so, the most accessed
blocks - the metadata blocks etc - will sit in DRAM, so this hottest data is
also served fast. I'm still going to benchmark a mixed system, but it's likely
in such a case the Flash-based system won't be much slower at all overall.

What this does mean is there's little point in doing off-heap caching when you
have Flash - the performance difference between the two models isn't worth
bothering with.

11. Work still to do
~~~~~~~~~~~~~~~~~~~~
The core cache is completely functional and appears to survive overnight runs
on the developer's machine, even under heavy eviction. The following work
still needs to be done, it's straightforward but just needs a bit of time:
  o HeapSize interface is not implemented yet.
  o Cache statistics dumping to the log is not implemented yet.
  o Clean up code, remove some debug methods
  o Remove some profanity and correctly format comments
  o JavaDoc for the majority of the classes and methods
  o Unit tests. I have some test code I've used to thrash the system as a
    whole as well as DirectArray/DirectHash but they are not formatted
    and runnable from JUnit or whatever.

12. Questions?
~~~~~~~~~~~~~~
Send me an email at neil@fusionio.com (work) or nacarson@hotmail.com (home)

