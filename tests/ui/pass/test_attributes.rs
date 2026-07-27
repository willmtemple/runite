//! Attribute placement must remain lint-clean.

#![deny(missing_docs, non_snake_case, unused_variables, warnings)]

/// The lint allowance belongs to the async implementation.
#[runite::test]
#[allow(non_snake_case, unused_variables)]
pub async fn PublicWrapperKeepsLintAndDocumentationAttributes() {
    let intentionally_unused = 42;
}

#[runite::test]
#[cfg_attr(
    all(),
    doc = "Conditional documentation belongs to the public wrapper.",
    allow(non_snake_case, unused_variables)
)]
pub async fn ConditionalWrapperAttributes() {
    let intentionally_unused = 42;
}

#[runite::test]
#[cfg(any())]
async fn cfg_disabled_test_is_removed_completely() {
    missing::code::must_not_be_resolved();
}

#[runite::test]
#[cfg_attr(all(), cfg(any()))]
async fn cfg_attr_disabled_test_is_removed_completely() {
    more_missing::code::must_not_be_resolved();
}

#[runite::test]
#[should_panic = "expected"]
async fn harness_attribute_reaches_wrapper() {
    panic!("expected");
}

async fn __runite_runtime_internal_no_helper_collision() {}

async fn __runite_implementation() {}

#[runite::test]
async fn no_helper_collision() {}

fn main() {
    let _future = __runite_runtime_internal_no_helper_collision();
    let _future = __runite_implementation();
}
