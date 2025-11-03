use std::{
    any::{Any, TypeId},
    marker::PhantomData,
};

use bevy::{
    ecs::{
        entity::Entity,
        query::FilteredAccessSet,
        resource::Resource,
        system::{
            IntoSystem, RegisteredSystemError, RunSystemError, System, SystemId, SystemInput,
            SystemParam,
        },
        world::{World, unsafe_world_cell::UnsafeWorldCell},
    },
    platform::collections::HashMap,
};

use crate::dynamic_bundle::DynamicBundle;

pub struct SystemRunnerBuilder<'w, Marker: Send + Sync + 'static> {
    world: &'w mut World,
    accesses: HashMap<TypeId, FilteredAccessSet>,
    total_access: FilteredAccessSet,
    systems: DynamicBundle,
    phantom_data: PhantomData<Marker>,
}

impl<'w, Marker: Send + Sync + 'static> SystemRunnerBuilder<'w, Marker> {
    pub fn new(world: &'w mut World) -> Self {
        Self {
            world,
            accesses: HashMap::new(),
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
        self.systems.add(system);
        self.total_access.extend(access.clone());
        self.accesses.insert(type_id, access);
    }

    pub fn build(self) {
        self.world.insert_resource(SystemRunnerResource {
            accesses: self.accesses,
            systems: self.systems,
            total_access: self.total_access,
            phantom_data: PhantomData::<Marker>,
        });
    }
}

#[derive(Resource)]
pub struct SystemRunnerResource<Marker: Send + Sync + 'static> {
    accesses: HashMap<TypeId, FilteredAccessSet>,
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
        _system_meta: &mut bevy::ecs::system::SystemMeta,
        component_access_set: &mut FilteredAccessSet,
        world: &mut World,
    ) {
        let resource = world
            .get_resource::<SystemRunnerResource<Marker>>()
            .unwrap();
        assert!(
            resource.total_access.is_compatible(component_access_set),
            "error[B0002]: SystemRunner conflicts with previously registered access."
        );
        component_access_set.extend(resource.total_access.clone());
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

#[cfg(test)]
mod tests {
    use bevy::ecs::{schedule::Schedule, system::ResMut};

    use super::*;

    #[derive(Resource, Default)]
    struct TestResource(Vec<usize>);

    #[test]
    fn basic_usage() {
        struct MySystems;
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
        todo!()
    }

    #[test]
    fn can_pass_in_lifetimed_input() {}

    #[test]
    #[should_panic(expected = "error[B0002]")]
    fn conflicts_with_resource_first() {
        struct MySystems;
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
        struct MySystems;
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
}
