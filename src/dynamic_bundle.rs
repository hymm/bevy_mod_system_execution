use bevy::utils::TypeIdMap;
use bevy::{platform::collections::hash_map::Entry, ptr::OwningPtr};
use std::{
    alloc::{Layout, alloc, dealloc},
    any::TypeId,
    mem::needs_drop,
    ptr::NonNull,
};

struct TypeDescriptor {
    type_id: Option<TypeId>,
    layout: Layout,
    // SAFETY: this function must be safe to call with pointers pointing to items of the type
    // this descriptor describes.
    // None if the underlying type doesn't need to be dropped
    drop: Option<for<'a> unsafe fn(OwningPtr<'a>)>,
}

impl TypeDescriptor {
    /// # Safety
    ///
    /// `x` must point to a valid value of type `T`.
    unsafe fn drop_ptr<T>(x: OwningPtr<'_>) {
        // SAFETY: Contract is required to be upheld by the caller.
        unsafe {
            x.drop_as::<T>();
        }
    }

    pub fn new<T: 'static>() -> Self {
        Self {
            type_id: Some(TypeId::of::<T>()),
            layout: Layout::new::<T>(),
            drop: needs_drop::<T>().then_some(Self::drop_ptr::<T> as _),
        }
    }
}

/// Heterogeneous data storage
pub struct DynamicBundle {
    data: NonNull<u8>,
    /// (ConponentInfo, offset)
    // Note to self: we need ComponentInfo because it stores the drop info
    info: Vec<(TypeDescriptor, usize)>,
    layout: Layout,
    /// offset to next free byte of `data`
    cursor: usize,
    /// Map a component id to the index
    indices: TypeIdMap<usize>,
}

/// TODO: require send sync for the components
unsafe impl Send for DynamicBundle {}
unsafe impl Sync for DynamicBundle {}

impl DynamicBundle {
    /// allocate with size `capacity`, if needed the dynamic bundle will reallocate.
    pub fn with_capacity(size: usize) -> DynamicBundle {
        // Safety: Layout is non zero sized.
        let ptr = unsafe { alloc(Layout::from_size_align(size, 1).unwrap()) };
        assert!(ptr != std::ptr::null_mut());
        DynamicBundle {
            // Safety: alloc only returns null if the allocation fails
            data: unsafe { NonNull::new_unchecked(ptr) },
            info: vec![],
            layout: Layout::from_size_align(0, 8).unwrap(),
            cursor: 0,
            indices: TypeIdMap::default(),
        }
    }

    pub fn size(&self) -> usize {
        self.layout.size()
    }

    /// Clear the DynmaicBuffer. This allows the allocation to be reused.
    pub fn clear(&mut self) {
        self.indices.clear();
        self.info.clear();
        self.cursor = 0;

        // TODO: we need to call the drop impl's of any stored data
    }

    /// insert `data` into the [`DynamicBundle`]. If the data already exists replace the old value
    pub fn add<T: 'static>(&mut self, mut data: T) {
        match self.indices.entry(TypeId::of::<T>()) {
            Entry::Occupied(occupied_entry) => {
                // if the data already exists we need to replace the value
                let index = *occupied_entry.get();
                let (desc, offset) = &mut self.info[index];

                // Safety: `offset` is garunteed to be in `data`
                let ptr = unsafe { self.data.byte_add(*offset) };
                if let Some(drop) = desc.drop {
                    // SAFETY: ptr is of the corrent type
                    unsafe {
                        drop(OwningPtr::new(ptr));
                    }
                }

                unsafe {
                    std::ptr::copy_nonoverlapping(&mut data as *mut T, ptr.as_ptr() as *mut T, 1);
                }
            }
            Entry::Vacant(vacant) => {
                let layout = Layout::new::<T>();
                let offset = align(self.cursor, layout.align());
                // unsafe { self.data.byte_add(self.cursor) }.align_offset(dbg!(layout.align()));
                let end = offset + self.cursor + layout.size();
                // if we don't have enough capacity or the alignment has changed we need to reallocate
                // Note: As long as the alignment only increases the data already placed in the allocation
                // will be aligned for their own layouts. i.e. increasing the alignment will not make the
                // data already written to become unaligned.
                if end > self.layout.size() || self.layout.align() < layout.align() {
                    let new_align = self.layout.align().max(layout.align());
                    let (new_ptr, new_layout) =
                        Self::grow(self.data, self.cursor, self.layout, end, new_align);
                    self.data = new_ptr;
                    self.layout = new_layout;
                }

                let addr = unsafe { self.data.as_ptr().add(offset) };
                unsafe { std::ptr::copy_nonoverlapping(&mut data as *mut T, addr as *mut T, 1) };

                vacant.insert(self.info.len());
                self.info.push((TypeDescriptor::new::<T>(), offset));
                self.cursor = end;
            }
        }
        // leak the data so drop is not called
        core::mem::forget(data);
    }

    /// grow the currrent allocation, returns the new size
    fn grow(
        old_ptr: NonNull<u8>,
        cursor: usize,
        old_layout: Layout,
        new_min_capacity: usize,
        align: usize,
    ) -> (NonNull<u8>, Layout) {
        let layout =
            Layout::from_size_align(new_min_capacity.next_power_of_two().max(64), align).unwrap();
        // SAFETY: max check above will always return a nonzero size
        let new_ptr = unsafe { alloc(layout) };
        assert!(new_ptr != std::ptr::null_mut(), "allocation failed");
        // SAFETY: asserted that the ptr was not null above
        let new_ptr = unsafe { NonNull::new_unchecked(new_ptr) };
        if old_layout.size() > 0 {
            // SAFETY:
            // * the pointers are from different allocations as so cannot overlap
            // * cursor is always within the different allocations
            unsafe { std::ptr::copy_nonoverlapping(old_ptr.as_ptr(), new_ptr.as_ptr(), cursor) };
            unsafe { dealloc(old_ptr.as_ptr(), old_layout) };
        }
        (new_ptr, layout)
    }

    /// Get a reference to a value that was previously stored
    pub fn get<T: 'static>(&self) -> Option<&T> {
        let type_id = TypeId::of::<T>();
        let index = self.indices.get(&type_id)?;
        let (_, offset) = self.info[*index];
        // SAFETY: offset is always within bounds
        let ptr = unsafe { self.data.add(offset) };
        // SAFETY: data was stored so that it could be dereferenced...
        Some(unsafe { &*(ptr.as_ptr() as *const T) })
    }

    /// Get a reference to a value that was previously stored
    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        let type_id = TypeId::of::<T>();
        let index = self.indices.get(&type_id)?;
        let (_, offset) = self.info[*index];
        // SAFETY: offset is always within bounds
        let ptr = unsafe { self.data.add(offset) };
        // SAFETY: data was stored so that it could be dereferenced...
        Some(unsafe { &mut *(ptr.as_ptr() as *mut T) })
    }

    /// returns an iterator over the type ids. Will skip any values that don't have type id's
    pub fn type_ids(&self) -> impl Iterator<Item = TypeId> + use<'_> {
        self.info.iter().filter_map(|(desc, _)| desc.type_id)
    }

    /// Returns an iterator over OwningPtr's that can be used to insert the data into the world
    pub fn consume(&mut self) -> impl Iterator<Item = OwningPtr<'_>> {
        // TODO: this method should probably be unsafe
        // TODO: should reset the memory?

        self.info.iter().map(|(_desc, offset)| {
            let ptr = unsafe { self.data.add(*offset) };
            unsafe { OwningPtr::new(ptr) }
        })
    }

    /// Returns true if there is no data stored.
    pub fn is_empty(&self) -> bool {
        self.cursor == 0
    }
}

impl Default for DynamicBundle {
    fn default() -> DynamicBundle {
        DynamicBundle {
            data: NonNull::dangling(),
            info: vec![],
            layout: Layout::from_size_align(0, 8).unwrap(),
            cursor: 0,
            indices: TypeIdMap::default(),
        }
    }
}

impl Drop for DynamicBundle {
    fn drop(&mut self) {
        if self.layout.size() > 0 {
            // TODO: we need to call the drop impl's of the stored values.
            for (desc, offset) in &mut self.info {
                let ptr = unsafe { self.data.add(*offset) };
                let Some(drop) = desc.drop else {
                    continue;
                };
                // SAFETY: ptr is aligned and is valid until deallocated below
                let ptr = unsafe { OwningPtr::new(ptr) };
                // SAFETY: ptr points to the same type as desc
                unsafe { drop(ptr) };
            }

            // Safety:
            // * layout matches what it was allocated with
            // * points to a valid allocation that was allocated from the global allocator
            unsafe { dealloc(self.data.as_ptr(), self.layout) };
        }
    }
}

fn align(x: usize, alignment: usize) -> usize {
    debug_assert!(alignment.is_power_of_two());
    (x + alignment - 1) & (!alignment + 1)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use bevy::{ecs::component::ComponentId, prelude::*};

    #[derive(Component, Clone, Debug)]
    struct DropCk(Arc<AtomicUsize>);
    impl DropCk {
        fn new_pair() -> (Self, Arc<AtomicUsize>) {
            let atomic = Arc::new(AtomicUsize::new(0));
            (DropCk(atomic.clone()), atomic)
        }
    }

    impl Drop for DropCk {
        fn drop(&mut self) {
            self.0.as_ref().fetch_add(1, Ordering::Relaxed);
        }
    }

    #[derive(Component)]
    struct C8(u8);
    #[derive(Component)]
    struct C16(u16);
    #[derive(Component)]
    struct C32(u32);
    #[derive(Component)]
    struct C64(u64);

    #[test]
    fn calls_drop_if_exists() {
        let (component, counter) = DropCk::new_pair();
        {
            let mut bundle = DynamicBundle::default();
            bundle.add(component);
        }
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn calls_drop_if_replaced() {
        let (component, counter) = DropCk::new_pair();

        let mut bundle = DynamicBundle::default();
        bundle.add(component);
        let (new_component, _new_counter) = DropCk::new_pair();
        bundle.add(new_component);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn can_read_data() {
        let mut bundle = DynamicBundle::default();
        bundle.add(C8(1));
        bundle.add(C16(2));
        bundle.add(C32(3));
        bundle.add(C64(4));

        assert_eq!(bundle.get::<C8>().unwrap().0, 1);
        assert_eq!(bundle.get::<C16>().unwrap().0, 2);
        assert_eq!(bundle.get::<C32>().unwrap().0, 3);
        assert_eq!(bundle.get::<C64>().unwrap().0, 4);
    }

    #[test]
    fn insert_on_entity() {
        let mut world = World::new();
        world.register_component::<C32>();
        let mut bundle = DynamicBundle::default();

        bundle.add(C32(10));

        let component_ids: Vec<ComponentId> = bundle
            .type_ids()
            .map(|type_id| world.components().get_id(type_id).unwrap())
            .collect();
        let mut entity = world.spawn_empty();
        // TODO: this is not safe since type_ids() can skip values
        unsafe { entity.insert_by_ids(&component_ids, bundle.consume()) };
        assert_eq!(entity.get::<C32>().unwrap().0, 10);
        assert!(bundle.is_empty());
    }

    #[test]
    fn failed_insert_on_entity() {
        // we need to make sure drop is called if insert on entity fails
        // also how to recover if only some of the data has been copied.
        todo!();
    }
}
