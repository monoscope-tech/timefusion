#[cfg(test)]
#[allow(warnings)] // Suppress warnings for the test module
mod sqllogictest_tests {

    #[tokio::test]
    async fn run_sqllogictest() -> anyhow::Result<()> {
        println!("sql logic test");
        Ok(())
    }
}
