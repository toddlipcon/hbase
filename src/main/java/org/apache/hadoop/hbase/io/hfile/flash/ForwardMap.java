package org.apache.hadoop.hbase.io.hfile.flash;

import java.util.Iterator;

import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;

// Need to get the data structure to iterate. For each item, get its offset.
// Need to be able to save a reference to each item. On iteration, we need to put referneces
// to things in various lists. Then free the stuff from that list later.
public interface ForwardMap extends Iterable<ForwardMap.Reference> {
    /* Our forward map has special requirements for EFFICIENT iteration - i.e. iteration,
     * and remembering referneces to iterated items, without creating gazillions of
     * heap objects in the process.
     *
     * When we iterate, we return object implementing the Reference iterface. This allows
     * us to get at all values of the stored data in the hash efficiently.
     *
     * We can use an instance of the ReferenceList interface, instantiated by the
     * factoryReferenceList() method, to efficiently save references in a collection-dependent
     * fashion.
     */
    public interface Reference {
        public void getEntryTo(FlashCache.Entry entry);
        public int getHashCode();
    }
    public interface ReferenceList extends Iterable<ForwardMap.Reference> {
        public int size();
        void add(Reference ref);
        Iterator<ForwardMap.Reference>iterator();
    }
    Iterator<ForwardMap.Reference>iterator();
    ReferenceList factoryReferenceList();

    /* Standard map-type methods for the forward map follow here.
     */
    boolean get(BlockCacheKey key,FlashCache.Entry out);
    void put(BlockCacheKey key,FlashCache.Entry entry);
    boolean containsKey(BlockCacheKey key);
    boolean remove(BlockCacheKey key);
    void remove(Reference ref);

    /* Save and load the forward map */
    void persistTo(java.io.ObjectOutputStream oos) throws java.io.IOException;
    void retrieveFrom(java.io.ObjectInputStream ois) throws java.io.IOException;
}
