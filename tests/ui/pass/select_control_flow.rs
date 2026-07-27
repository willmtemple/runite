#![deny(warnings)]

async fn choose() -> usize {
    runite::select! {
        value = async { 1usize } => async move { value }.await,
        value = async { 2usize } => value,
    }
}

async fn return_from_handler() -> usize {
    runite::select! {
        _ = async {} => return 3,
    }
}

async fn break_from_handler() -> usize {
    loop {
        runite::select! {
            value = async { 4usize } => break value,
        }
    }
}

struct Resource;

impl Resource {
    async fn operation(&mut self) {}
}

async fn reuse_loop_resource(resource: &mut Resource) {
    loop {
        let operation = resource.operation();
        runite::select! {
            _ = operation => break,
        }
    }
}

async fn caller_identifiers_do_not_collide() -> usize {
    let __runite_select_state = 40usize;
    let __runite_select_winner = 2usize;
    runite::select! {
        _ = async {} => __runite_select_state + __runite_select_winner,
    }
}

async fn mutable_binding_patterns() -> (usize, usize, usize) {
    let first = runite::select! {
        Some(mut value) = async { Some(1usize) } => {
            value += 1;
            value
        },
    };
    let second = runite::select! {
        ref mut value = async { 2usize } => {
            *value += 1;
            *value
        },
    };
    let mut referent = 3usize;
    let third = runite::select! {
        &mut value = async { &mut referent } => value + 1,
    };
    (first, second, third)
}

struct GuardedResource;

impl GuardedResource {
    fn enabled(&self) -> bool {
        true
    }

    async fn operation(&mut self) {}
}

async fn all_guards_precede_future_construction(resource: &mut GuardedResource) {
    runite::select! {
        _ = resource.operation() => (),
        _ = std::future::pending::<()>(), if resource.enabled() => (),
    }
}

const EXPECTED: usize = 7;

async fn bare_variant_and_constant_patterns() -> (&'static str, &'static str) {
    let none = runite::select! {
        None = async { Some(1usize) } => "matched None",
        else => "None mismatch",
    };
    let constant = runite::select! {
        EXPECTED = async { 8usize } => "matched constant",
        else => "constant mismatch",
    };
    (none, constant)
}

fn main() {
    let _future = choose();
    let _future = return_from_handler();
    let _future = break_from_handler();
    let mut resource = Resource;
    let _future = reuse_loop_resource(&mut resource);
    let _future = caller_identifiers_do_not_collide();
    let _future = mutable_binding_patterns();
    let mut guarded = GuardedResource;
    let _future = all_guards_precede_future_construction(&mut guarded);
    let _future = bare_variant_and_constant_patterns();
}
