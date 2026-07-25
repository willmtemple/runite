//! Compile-time coverage for attributes on generated test wrappers.

#![deny(missing_docs, non_snake_case, unused_variables, warnings)]

/// Direct documentation and lint attributes apply to the public wrapper.
#[runite::test]
#[allow(non_snake_case, unused_variables)]
pub async fn PublicWrapperKeepsDirectAttributes() {
    let intentionally_unused = 42;
}

#[runite::test]
#[cfg_attr(
    all(),
    doc = "Conditional documentation and lints apply to the public wrapper.",
    allow(non_snake_case, unused_variables)
)]
pub async fn PublicWrapperKeepsConditionalAttributes() {
    let intentionally_unused = 42;
}

#[runite::test]
#[cfg(any())]
pub async fn DisabledPublicWrapperIsRemoved() {
    unresolved::tokens::must_not_be_checked();
}

#[runite::test]
#[cfg_attr(all(), cfg(any()))]
pub async fn ConditionallyDisabledPublicWrapperIsRemoved() {
    more_unresolved::tokens::must_not_be_checked();
}
