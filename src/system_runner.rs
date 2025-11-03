use std::{any::TypeId, marker::PhantomData, ptr::NonNull};

use bevy::{
    ecs::{
        query::FilteredAccessSet,
        resource::Resource,
        system::{IntoSystem, ResMut, RunSystemError, System, SystemInput, SystemParam},
        world::{DeferredWorld, Mut, World, unsafe_world_cell::UnsafeWorldCell},
    },
    platform::collections::HashMap,
};

use crate::dynamic_bundle::DynamicBundle;

pub struct SystemRunnerBuilder<'w, Marker: Send + Sync + 'static> {
    world: &'w mut World,
    accesses: HashMap<TypeId, FilteredAccessSet>,
    is_send: bool,
    is_exclusive: bool,
    has_deferred: bool,
    apply_deffereds: HashMap<TypeId, unsafe fn(NonNull<u8>, &mut World)>,
    queue_deffereds: HashMap<TypeId, unsafe fn(NonNull<u8>, DeferredWorld)>,
    total_access: FilteredAccessSet,
    systems: DynamicBundle,
    phantom_data: PhantomData<Marker>,
}

impl<'w, Marker: Send + Sync + 'static> SystemRunnerBuilder<'w, Marker> {
    pub fn new(world: &'w mut World) -> Self {
        Self {
            world,
            accesses: HashMap::new(),
            is_send: true,
            is_exclusive: false,
            has_deferred: false,
            apply_deffereds: HashMap::new(),
            queue_deffereds: HashMap::new(),
            total_access: FilteredAccessSet::default(),
            systems: DynamicBundle::with_capacity(0),
            phantom_data: PhantomData,
        }
    }

    pub fn add_system<I, O, M, S>(&mut self, system: S)
    where
        I: SystemInput + 'static,
        O: 'static,
        S: IntoSystem<I, O, M> + 'static,
    {
        let type_id = TypeId::of::<S::System>();
        let mut system = IntoSystem::into_system(system);
        let access = system.initialize(self.world);
        if system.is_exclusive() {
            unimplemented!("currently cannot support exclusive systems");
        }
        if system.has_deferred() {
            self.has_deferred = true;
        }
        if !system.is_send() {
            self.is_send = false;
        }
        self.systems.add(system);
        self.total_access.extend(access.clone());
        self.accesses.insert(type_id, access);
        self.apply_deffereds
            .insert(type_id, <S::System as ApplyBuffers>::apply_deferred);
        self.queue_deffereds
            .insert(type_id, <S::System as ApplyBuffers>::queue_deferred);
    }

    pub fn build(self) {
        self.world.insert_resource(SystemRunnerResource {
            is_send: self.is_send,
            is_exclusive: self.is_exclusive,
            has_deferred: self.has_deferred,
            accesses: self.accesses,
            apply_deffereds: self.apply_deffereds,
            queue_deffereds: self.queue_deffereds,
            systems: self.systems,
            total_access: self.total_access,
            phantom_data: PhantomData::<Marker>,
        });
    }
}

#[derive(Resource)]
pub struct SystemRunnerResource<Marker: Send + Sync + 'static> {
    is_send: bool,
    is_exclusive: bool,
    has_deferred: bool,
    accesses: HashMap<TypeId, FilteredAccessSet>,
    apply_deffereds: HashMap<TypeId, unsafe fn(NonNull<u8>, &mut World)>,
    queue_deffereds: HashMap<TypeId, unsafe fn(NonNull<u8>, DeferredWorld)>,
    systems: DynamicBundle,
    total_access: FilteredAccessSet,
    phantom_data: PhantomData<Marker>,
}

pub struct SystemRunner<'w, Marker: Send + Sync + 'static> {
    world: UnsafeWorldCell<'w>,
    systems: &'w mut DynamicBundle,
    phantom_data: PhantomData<Marker>,
}

unsafe impl<Marker: Send + Sync + 'static> SystemParam for SystemRunner<'_, Marker> {
    type State = ();

    type Item<'world, 'state> = SystemRunner<'world, Marker>;

    fn init_state(_world: &mut World) -> Self::State {}

    fn init_access(
        _state: &Self::State,
        system_meta: &mut bevy::ecs::system::SystemMeta,
        component_access_set: &mut FilteredAccessSet,
        world: &mut World,
    ) {
        let res_mut_state = ResMut::<SystemRunnerResource<Marker>>::init_state(world);
        ResMut::<SystemRunnerResource<Marker>>::init_access(
            &res_mut_state,
            system_meta,
            component_access_set,
            world,
        );

        let resource = world
            .get_resource::<SystemRunnerResource<Marker>>()
            .unwrap();
        assert!(
            resource.total_access.is_compatible(component_access_set),
            "error[B0002]: SystemRunner conflicts with previously registered access."
        );
        component_access_set.extend(resource.total_access.clone());

        if !resource.is_send {
            system_meta.set_non_send();
        }
        if resource.has_deferred {
            system_meta.set_has_deferred();
        }
    }

    unsafe fn get_param<'world, 'state>(
        _state: &'state mut Self::State,
        _system_meta: &bevy::ecs::system::SystemMeta,
        world: UnsafeWorldCell<'world>,
        _change_tick: bevy::ecs::component::Tick,
    ) -> Self::Item<'world, 'state> {
        let resource = unsafe {
            world
                .get_resource_mut::<SystemRunnerResource<Marker>>()
                .unwrap()
                .into_inner()
        };

        SystemRunner {
            world,
            systems: &mut resource.systems,
            phantom_data: PhantomData,
        }
    }

    fn apply(
        _state: &mut Self::State,
        _system_meta: &bevy::ecs::system::SystemMeta,
        world: &mut World,
    ) {
        world.resource_scope(|world, mut resource: Mut<SystemRunnerResource<Marker>>| {
            let SystemRunnerResource {
                ref mut systems,
                ref apply_deffereds,
                ..
            } = *resource;
            for (type_id, ptr) in systems.iter_mut() {
                let apply_deferred = apply_deffereds.get(&type_id.unwrap()).unwrap();
                unsafe {
                    apply_deferred(ptr, world);
                }
            }
        });
    }

    fn queue(
        state: &mut Self::State,
        system_meta: &bevy::ecs::system::SystemMeta,
        world: bevy::ecs::world::DeferredWorld,
    ) {
        // not possible to implement safely due to needing mutable access to the SystemRunnerResource
        unimplemented!("Cannot use SystemRunner with observers");
    }
}

impl<'w, Marker: Send + Sync + 'static> SystemRunner<'w, Marker> {
    pub fn run_system_with<I, O, M, S>(
        &mut self,
        // we just use this to determine the type
        _system: S,
        input: <I as SystemInput>::Inner<'_>,
    ) -> Result<O, RunSystemError>
    where
        I: SystemInput,
        S: IntoSystem<I, O, M> + 'static,
    {
        // TODO: add a better error with the system name
        let system = self.systems.get_mut::<S::System>().unwrap();
        // Safety:
        // - the accesses needed to run this system were registered by SystemRunner

        unsafe { system.run_unsafe(input, self.world) }
    }

    #[inline]
    pub fn run_system<O, M, S>(&mut self, system: S) -> Result<O, RunSystemError>
    where
        S: IntoSystem<(), O, M> + 'static,
    {
        self.run_system_with(system, ())
    }
}

trait ApplyBuffers {
    unsafe fn apply_deferred(ptr: NonNull<u8>, world: &mut World);
    unsafe fn queue_deferred(ptr: NonNull<u8>, world: DeferredWorld);
}

impl<S> ApplyBuffers for S
where
    S: System,
{
    // Safety
    // - `ptr` is a valid pointer to `S`
    unsafe fn apply_deferred(ptr: NonNull<u8>, world: &mut World) {
        let mut ptr = ptr.cast::<S>();
        // Safety: upheld by the caller
        unsafe {
            System::apply_deferred(ptr.as_mut(), world);
        }
    }

    // Safety
    // - `ptr` is a valid pointer to `S`
    unsafe fn queue_deferred(ptr: NonNull<u8>, world: DeferredWorld) {
        let mut ptr = ptr.cast::<S>();
        // Safety: upheld by the caller
        unsafe {
            System::queue_deferred(ptr.as_mut(), world);
        }
    }
}

#[cfg(test)]
mod tests {
    use bevy::ecs::{
        schedule::Schedule,
        system::{Commands, Local, Res, ResMut},
    };

    use super::*;

    struct MySystems;

    #[derive(Resource, Default)]
    struct TestResource(Vec<usize>);

    #[test]
    fn basic_usage() {
        fn system_1(mut r: ResMut<TestResource>) {
            r.0.push(1);
        }
        fn system_2(mut r: ResMut<TestResource>) {
            r.0.push(2);
        }
        let mut world = World::new();
        world.init_resource::<TestResource>();
        let mut builder = SystemRunnerBuilder::<MySystems>::new(&mut world);
        builder.add_system(system_1);
        builder.add_system(system_2);
        builder.build();

        fn runner_system(mut system_runner: SystemRunner<MySystems>) {
            system_runner.run_system(system_1).unwrap();
            system_runner.run_system(system_2).unwrap();
        }

        let mut schedule = Schedule::default();
        schedule.add_systems(runner_system);
        schedule.run(&mut world);
        assert_eq!(&world.resource::<TestResource>().0, &[1, 2]);
    }

    #[test]
    fn system_state_is_cached() {
        fn system_1(mut r: ResMut<TestResource>, mut count: Local<usize>) {
            *count += 1;
            r.0.push(*count);
        }
        let mut world = World::new();
        world.init_resource::<TestResource>();
        let mut builder = SystemRunnerBuilder::<MySystems>::new(&mut world);
        builder.add_system(system_1);
        builder.build();

        fn runner_system(mut system_runner: SystemRunner<MySystems>) {
            system_runner.run_system(system_1).unwrap();
            system_runner.run_system(system_1).unwrap();
        }

        let mut schedule = Schedule::default();
        schedule.add_systems(runner_system);
        schedule.run(&mut world);
        assert_eq!(&world.resource::<TestResource>().0, &[1, 2]);
    }

    #[test]
    fn can_pass_in_lifetimed_input() {
        todo!()
    }

    #[test]
    #[should_panic(expected = "error[B0002]")]
    fn conflicts_with_resource_first() {
        fn system_1(mut r: ResMut<TestResource>) {
            r.0.push(1);
        }
        let mut world = World::new();
        world.init_resource::<TestResource>();
        let mut builder = SystemRunnerBuilder::<MySystems>::new(&mut world);
        builder.add_system(system_1);
        builder.build();

        fn resource_first(_res: ResMut<TestResource>, _system_runner: SystemRunner<MySystems>) {}

        let mut system = IntoSystem::into_system(resource_first);
        system.initialize(&mut world);
    }

    #[test]
    #[should_panic(expected = "error[B0002]")]
    fn conflicts_with_resource_second() {
        fn system_1(mut r: ResMut<TestResource>) {
            r.0.push(1);
        }
        let mut world = World::new();
        world.init_resource::<TestResource>();
        let mut builder = SystemRunnerBuilder::<MySystems>::new(&mut world);
        builder.add_system(system_1);
        builder.build();

        fn resource_second(_system_runner: SystemRunner<MySystems>, _res: ResMut<TestResource>) {}

        let mut system = IntoSystem::into_system(resource_second);
        system.initialize(&mut world);
    }

    #[test]
    fn will_apply_commands() {
        fn system_1(mut commands: Commands) {
            commands.init_resource::<TestResource>();
        }

        let mut world = World::new();
        let mut builder = SystemRunnerBuilder::<MySystems>::new(&mut world);
        builder.add_system(system_1);
        builder.build();

        fn runner_system(mut system_runner: SystemRunner<MySystems>) {
            system_runner.run_system(system_1).unwrap();
        }

        let mut system = IntoSystem::into_system(runner_system);
        system.initialize(&mut world);
        system.run((), &mut world).unwrap();
        assert!(world.get_resource::<TestResource>().is_some());
    }

    #[test]
    #[should_panic(expected = "error[B0002]")]
    fn param_conflicts_with_resource_first() {
        let mut world = World::new();
        let builder = SystemRunnerBuilder::<MySystems>::new(&mut world);
        builder.build();

        fn runner_system(
            _resource: Res<SystemRunnerResource<MySystems>>,
            _system_runner: SystemRunner<MySystems>,
        ) {
        }

        let mut system = IntoSystem::into_system(runner_system);
        system.initialize(&mut world);
    }

    #[test]
    #[should_panic(expected = "error[B0002]")]
    fn param_conflicts_with_resource_second() {
        let mut world = World::new();
        let builder = SystemRunnerBuilder::<MySystems>::new(&mut world);
        builder.build();

        fn runner_system(
            _system_runner: SystemRunner<MySystems>,
            _resource: Res<SystemRunnerResource<MySystems>>,
        ) {
        }

        let mut system = IntoSystem::into_system(runner_system);
        system.initialize(&mut world);
    }

    #[test]
    fn registers_as_exclusive() {
        todo!();
    }

    #[test]
    fn registers_as_non_send() {}

    #[test]
    fn registers_as_has_deferred() {}
}
