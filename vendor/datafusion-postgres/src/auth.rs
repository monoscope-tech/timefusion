use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use pgwire::api::auth::{AuthSource, LoginInfo, Password};
use pgwire::error::{PgWireError, PgWireResult};
use tokio::sync::RwLock;

use datafusion_pg_catalog::pg_catalog::context::*;

/// Authentication manager that handles users and roles
#[derive(Debug, Clone)]
pub struct AuthManager {
    users: Arc<RwLock<HashMap<String, User>>>,
    roles: Arc<RwLock<HashMap<String, Role>>>,
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthManager {
    pub fn new() -> Self {
        let mut users = HashMap::new();
        // Initialize with default postgres superuser
        let postgres_user = User {
            username: "postgres".to_string(),
            password_hash: "".to_string(), // Empty password for now
            roles: vec!["postgres".to_string()],
            is_superuser: true,
            can_login: true,
            connection_limit: None,
        };
        users.insert(postgres_user.username.clone(), postgres_user);

        let mut roles = HashMap::new();
        let postgres_role = Role {
            name: "postgres".to_string(),
            is_superuser: true,
            can_login: true,
            can_create_db: true,
            can_create_role: true,
            can_create_user: true,
            can_replication: true,
            grants: vec![Grant {
                permission: Permission::All,
                resource: ResourceType::All,
                granted_by: "system".to_string(),
                with_grant_option: true,
            }],
            inherited_roles: vec![],
        };
        roles.insert(postgres_role.name.clone(), postgres_role);

        AuthManager {
            users: Arc::new(RwLock::new(users)),
            roles: Arc::new(RwLock::new(roles)),
        }
    }

    /// Add a new user to the system
    pub async fn add_user(&self, user: User) -> PgWireResult<()> {
        let mut users = self.users.write().await;
        users.insert(user.username.clone(), user);
        Ok(())
    }

    /// Add a new role to the system
    pub async fn add_role(&self, role: Role) -> PgWireResult<()> {
        let mut roles = self.roles.write().await;
        roles.insert(role.name.clone(), role);
        Ok(())
    }

    /// Authenticate a user with username and password
    pub async fn authenticate(&self, username: &str, password: &str) -> PgWireResult<bool> {
        let users = self.users.read().await;

        if let Some(user) = users.get(username) {
            if !user.can_login {
                return Ok(false);
            }

            // For now, accept empty password or any password for existing users
            // In production, this should use proper password hashing (bcrypt, etc.)
            if user.password_hash.is_empty() || password == user.password_hash {
                return Ok(true);
            }
        }

        // If user doesn't exist, check if we should create them dynamically
        // For now, only accept known users
        Ok(false)
    }

    /// Get user information
    pub async fn get_user(&self, username: &str) -> Option<User> {
        let users = self.users.read().await;
        users.get(username).cloned()
    }

    /// Get role information
    pub async fn get_role(&self, role_name: &str) -> Option<Role> {
        let roles = self.roles.read().await;
        roles.get(role_name).cloned()
    }

    /// Check if user has a specific role
    pub async fn user_has_role(&self, username: &str, role_name: &str) -> bool {
        if let Some(user) = self.get_user(username).await {
            return user.roles.contains(&role_name.to_string()) || user.is_superuser;
        }
        false
    }

    /// List all users (for administrative purposes)
    pub async fn list_users(&self) -> Vec<String> {
        let users = self.users.read().await;
        users.keys().cloned().collect()
    }

    /// List all roles (for administrative purposes)
    pub async fn list_roles(&self) -> Vec<String> {
        let roles = self.roles.read().await;
        roles.keys().cloned().collect()
    }

    /// Grant permission to a role
    pub async fn grant_permission(
        &self,
        role_name: &str,
        permission: Permission,
        resource: ResourceType,
        granted_by: &str,
        with_grant_option: bool,
    ) -> PgWireResult<()> {
        let mut roles = self.roles.write().await;

        if let Some(role) = roles.get_mut(role_name) {
            let grant = Grant {
                permission,
                resource,
                granted_by: granted_by.to_string(),
                with_grant_option,
            };
            role.grants.push(grant);
            Ok(())
        } else {
            Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42704".to_string(), // undefined_object
                    format!("role \"{role_name}\" does not exist"),
                ),
            )))
        }
    }

    /// Revoke permission from a role
    pub async fn revoke_permission(
        &self,
        role_name: &str,
        permission: Permission,
        resource: ResourceType,
    ) -> PgWireResult<()> {
        let mut roles = self.roles.write().await;

        if let Some(role) = roles.get_mut(role_name) {
            role.grants
                .retain(|grant| !(grant.permission == permission && grant.resource == resource));
            Ok(())
        } else {
            Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42704".to_string(), // undefined_object
                    format!("role \"{role_name}\" does not exist"),
                ),
            )))
        }
    }

    /// Check if a user has a specific permission on a resource
    pub async fn check_permission(
        &self,
        username: &str,
        permission: Permission,
        resource: ResourceType,
    ) -> bool {
        // Superusers have all permissions
        if let Some(user) = self.get_user(username).await {
            if user.is_superuser {
                return true;
            }

            // Check permissions for each role the user has
            for role_name in &user.roles {
                if let Some(role) = self.get_role(role_name).await {
                    // Superuser role has all permissions
                    if role.is_superuser {
                        return true;
                    }

                    // Check direct grants
                    for grant in &role.grants {
                        if self.permission_matches(&grant.permission, &permission)
                            && self.resource_matches(&grant.resource, &resource)
                        {
                            return true;
                        }
                    }

                    // Check inherited roles recursively
                    for inherited_role in &role.inherited_roles {
                        if self
                            .check_role_permission(inherited_role, &permission, &resource)
                            .await
                        {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    /// Check if a role has a specific permission (helper for recursive checking)
    fn check_role_permission<'a>(
        &'a self,
        role_name: &'a str,
        permission: &'a Permission,
        resource: &'a ResourceType,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            if let Some(role) = self.get_role(role_name).await {
                if role.is_superuser {
                    return true;
                }

                // Check direct grants
                for grant in &role.grants {
                    if self.permission_matches(&grant.permission, permission)
                        && self.resource_matches(&grant.resource, resource)
                    {
                        return true;
                    }
                }

                // Check inherited roles
                for inherited_role in &role.inherited_roles {
                    if self
                        .check_role_permission(inherited_role, permission, resource)
                        .await
                    {
                        return true;
                    }
                }
            }

            false
        })
    }

    /// Check if a permission grant matches the requested permission
    fn permission_matches(&self, grant_permission: &Permission, requested: &Permission) -> bool {
        grant_permission == requested || matches!(grant_permission, Permission::All)
    }

    /// Check if a resource grant matches the requested resource
    fn resource_matches(&self, grant_resource: &ResourceType, requested: &ResourceType) -> bool {
        match (grant_resource, requested) {
            // Exact match
            (a, b) if a == b => true,
            // All resource type grants access to everything
            (ResourceType::All, _) => true,
            // Schema grants access to all tables in that schema
            (ResourceType::Schema(schema), ResourceType::Table(table)) => {
                // For simplicity, assume table names are schema.table format
                table.starts_with(&format!("{schema}."))
            }
            _ => false,
        }
    }

    /// Add role inheritance
    pub async fn add_role_inheritance(
        &self,
        child_role: &str,
        parent_role: &str,
    ) -> PgWireResult<()> {
        let mut roles = self.roles.write().await;

        if let Some(child) = roles.get_mut(child_role) {
            if !child.inherited_roles.contains(&parent_role.to_string()) {
                child.inherited_roles.push(parent_role.to_string());
            }
            Ok(())
        } else {
            Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42704".to_string(), // undefined_object
                    format!("role \"{child_role}\" does not exist"),
                ),
            )))
        }
    }

    /// Remove role inheritance
    pub async fn remove_role_inheritance(
        &self,
        child_role: &str,
        parent_role: &str,
    ) -> PgWireResult<()> {
        let mut roles = self.roles.write().await;

        if let Some(child) = roles.get_mut(child_role) {
            child.inherited_roles.retain(|role| role != parent_role);
            Ok(())
        } else {
            Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_string(),
                    "42704".to_string(), // undefined_object
                    format!("role \"{child_role}\" does not exist"),
                ),
            )))
        }
    }

    /// Create a new role with specific capabilities
    pub async fn create_role(&self, config: RoleConfig) -> PgWireResult<()> {
        let role = Role {
            name: config.name.clone(),
            is_superuser: config.is_superuser,
            can_login: config.can_login,
            can_create_db: config.can_create_db,
            can_create_role: config.can_create_role,
            can_create_user: config.can_create_user,
            can_replication: config.can_replication,
            grants: vec![],
            inherited_roles: vec![],
        };

        self.add_role(role).await
    }

    /// Create common predefined roles
    pub async fn create_predefined_roles(&self) -> PgWireResult<()> {
        // Read-only role
        self.create_role(RoleConfig {
            name: "readonly".to_string(),
            is_superuser: false,
            can_login: false,
            can_create_db: false,
            can_create_role: false,
            can_create_user: false,
            can_replication: false,
        })
        .await?;

        self.grant_permission(
            "readonly",
            Permission::Select,
            ResourceType::All,
            "system",
            false,
        )
        .await?;

        // Read-write role
        self.create_role(RoleConfig {
            name: "readwrite".to_string(),
            is_superuser: false,
            can_login: false,
            can_create_db: false,
            can_create_role: false,
            can_create_user: false,
            can_replication: false,
        })
        .await?;

        self.grant_permission(
            "readwrite",
            Permission::Select,
            ResourceType::All,
            "system",
            false,
        )
        .await?;

        self.grant_permission(
            "readwrite",
            Permission::Insert,
            ResourceType::All,
            "system",
            false,
        )
        .await?;

        self.grant_permission(
            "readwrite",
            Permission::Update,
            ResourceType::All,
            "system",
            false,
        )
        .await?;

        self.grant_permission(
            "readwrite",
            Permission::Delete,
            ResourceType::All,
            "system",
            false,
        )
        .await?;

        // Database admin role
        self.create_role(RoleConfig {
            name: "dbadmin".to_string(),
            is_superuser: false,
            can_login: true,
            can_create_db: true,
            can_create_role: false,
            can_create_user: false,
            can_replication: false,
        })
        .await?;

        self.grant_permission(
            "dbadmin",
            Permission::All,
            ResourceType::All,
            "system",
            true,
        )
        .await?;

        Ok(())
    }
}

#[async_trait]
impl PgCatalogContextProvider for AuthManager {
    // retrieve all database role names
    async fn roles(&self) -> Vec<String> {
        self.list_roles().await
    }

    // retrieve database role information
    async fn role(&self, name: &str) -> Option<Role> {
        self.get_role(name).await
    }
}

/// AuthSource implementation for integration with pgwire authentication
/// Provides proper password-based authentication instead of custom startup handler
#[derive(Clone, Debug)]
pub struct DfAuthSource {
    pub auth_manager: Arc<AuthManager>,
}

impl DfAuthSource {
    pub fn new(auth_manager: Arc<AuthManager>) -> Self {
        DfAuthSource { auth_manager }
    }
}

#[async_trait]
impl AuthSource for DfAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        if let Some(username) = login.user() {
            // Check if user exists in our RBAC system
            if let Some(user) = self.auth_manager.get_user(username).await {
                if user.can_login {
                    // Return the stored password hash for authentication
                    // The pgwire authentication handlers (cleartext/md5/scram) will
                    // handle the actual password verification process
                    Ok(Password::new(None, user.password_hash.into_bytes()))
                } else {
                    Err(PgWireError::UserError(Box::new(
                        pgwire::error::ErrorInfo::new(
                            "FATAL".to_string(),
                            "28000".to_string(), // invalid_authorization_specification
                            format!("User \"{username}\" is not allowed to login"),
                        ),
                    )))
                }
            } else {
                Err(PgWireError::UserError(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "FATAL".to_string(),
                        "28P01".to_string(), // invalid_password
                        format!("password authentication failed for user \"{username}\""),
                    ),
                )))
            }
        } else {
            Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "FATAL".to_string(),
                    "28P01".to_string(), // invalid_password
                    "No username provided in login request".to_string(),
                ),
            )))
        }
    }
}

// REMOVED: Custom startup handler approach
//
// Instead of implementing a custom StartupHandler, use the proper pgwire authentication:
//
// For cleartext authentication:
// ```rust
// use pgwire::api::auth::cleartext::CleartextStartupHandler;
//
// let auth_source = Arc::new(DfAuthSource::new(auth_manager));
// let authenticator = CleartextStartupHandler::new(
//     auth_source,
//     Arc::new(DefaultServerParameterProvider::default())
// );
// ```
//
// For MD5 authentication:
// ```rust
// use pgwire::api::auth::md5::MD5StartupHandler;
//
// let auth_source = Arc::new(DfAuthSource::new(auth_manager));
// let authenticator = MD5StartupHandler::new(
//     auth_source,
//     Arc::new(DefaultServerParameterProvider::default())
// );
// ```
//
// For SCRAM authentication (requires "server-api-scram" feature):
// ```rust
// use pgwire::api::auth::scram::SASLScramAuthStartupHandler;
//
// let auth_source = Arc::new(DfAuthSource::new(auth_manager));
// let authenticator = SASLScramAuthStartupHandler::new(
//     auth_source,
//     Arc::new(DefaultServerParameterProvider::default())
// );
// ```

/// Simple AuthSource implementation that accepts any user with empty password
#[derive(Debug)]
pub struct SimpleAuthSource {
    auth_manager: Arc<AuthManager>,
}

impl SimpleAuthSource {
    pub fn new(auth_manager: Arc<AuthManager>) -> Self {
        SimpleAuthSource { auth_manager }
    }
}

#[async_trait]
impl AuthSource for SimpleAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let username = login.user().unwrap_or("anonymous");

        // Check if user exists and can login
        if let Some(user) = self.auth_manager.get_user(username).await {
            if user.can_login {
                // Return empty password for now (no authentication required)
                return Ok(Password::new(None, vec![]));
            }
        }

        // For postgres user, always allow
        if username == "postgres" {
            return Ok(Password::new(None, vec![]));
        }

        // User not found or cannot login
        Err(PgWireError::UserError(Box::new(
            pgwire::error::ErrorInfo::new(
                "FATAL".to_string(),
                "28P01".to_string(), // invalid_password
                format!("password authentication failed for user \"{username}\""),
            ),
        )))
    }
}

/// Helper function to create auth source with auth manager
pub fn create_auth_source(auth_manager: Arc<AuthManager>) -> SimpleAuthSource {
    SimpleAuthSource::new(auth_manager)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_auth_manager_creation() {
        let auth_manager = AuthManager::new();

        // Wait a bit for the default user to be added
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let users = auth_manager.list_users().await;
        assert!(users.contains(&"postgres".to_string()));
    }

    #[tokio::test]
    async fn test_user_authentication() {
        let auth_manager = AuthManager::new();

        // Wait for initialization
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Test postgres user authentication
        assert!(auth_manager.authenticate("postgres", "").await.unwrap());
        assert!(!auth_manager
            .authenticate("nonexistent", "password")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_role_management() {
        let auth_manager = AuthManager::new();

        // Wait for initialization
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Test role checking
        assert!(auth_manager.user_has_role("postgres", "postgres").await);
        assert!(auth_manager.user_has_role("postgres", "any_role").await); // superuser
    }
}
