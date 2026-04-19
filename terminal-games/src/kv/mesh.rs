use std::sync::Arc;

use async_trait::async_trait;

use crate::mesh::{Mesh, NodeId};

use super::{KvBackend, KvError, KvKey, KvListPage};

pub fn load_mesh_backend(
    mesh: Mesh,
    leader: NodeId,
    local_backend: Option<Arc<dyn KvBackend>>,
) -> anyhow::Result<Arc<dyn KvBackend>> {
    if mesh.node() == leader && local_backend.is_none() {
        anyhow::bail!("kv leader node requires a local KV backend");
    }
    Ok(Arc::new(MeshKvBackend {
        mesh,
        leader,
        local_backend,
    }))
}

#[derive(Clone)]
struct MeshKvBackend {
    mesh: Mesh,
    leader: NodeId,
    local_backend: Option<Arc<dyn KvBackend>>,
}

#[async_trait]
impl KvBackend for MeshKvBackend {
    async fn get(&self, app_id: u64, key: KvKey) -> Result<Option<Vec<u8>>, KvError> {
        if self.mesh.node() == self.leader {
            self.local_backend
                .as_ref()
                .ok_or(KvError::Unavailable)?
                .get(app_id, key)
                .await
        } else {
            self.mesh
                .execute_kv_get_on_node(self.leader, app_id, key)
                .await
        }
    }

    async fn exec(&self, app_id: u64, commands: Vec<super::KvCommand>) -> Result<(), KvError> {
        if self.mesh.node() == self.leader {
            self.local_backend
                .as_ref()
                .ok_or(KvError::Unavailable)?
                .exec(app_id, commands)
                .await
        } else {
            self.mesh
                .execute_kv_exec_on_node(self.leader, app_id, commands)
                .await
        }
    }

    async fn list_page(
        &self,
        app_id: u64,
        prefix: KvKey,
        start: Option<KvKey>,
        end: Option<KvKey>,
        after: Option<KvKey>,
    ) -> Result<KvListPage, KvError> {
        if self.mesh.node() == self.leader {
            self.local_backend
                .as_ref()
                .ok_or(KvError::Unavailable)?
                .list_page(app_id, prefix, start, end, after)
                .await
        } else {
            self.mesh
                .execute_kv_list_page_on_node(self.leader, app_id, prefix, start, end, after)
                .await
        }
    }

    async fn storage_used(&self, app_id: u64) -> Result<u64, KvError> {
        if self.mesh.node() == self.leader {
            self.local_backend
                .as_ref()
                .ok_or(KvError::Unavailable)?
                .storage_used(app_id)
                .await
        } else {
            self.mesh
                .execute_kv_storage_used_on_node(self.leader, app_id)
                .await
        }
    }
}
