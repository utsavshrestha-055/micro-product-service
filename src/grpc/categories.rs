use crate::{
    errors,
    grpc::{page_offset, page_size, parse_uuid},
    proto::{
        Category, CategoryId, CategoryInput, CategoryList, CategoryResponse, OperationResponse,
        PageRequest, category_service_server::CategoryService,
    },
};
use sqlx::PgPool;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct CategoryGrpc {
    pool: PgPool,
}
impl CategoryGrpc {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
fn map(
    r: (
        uuid::Uuid,
        String,
        String,
        String,
        Option<uuid::Uuid>,
        bool,
        chrono::DateTime<chrono::Utc>,
        chrono::DateTime<chrono::Utc>,
    ),
) -> Category {
    Category {
        id: r.0.to_string(),
        name: r.1,
        slug: r.2,
        description: r.3,
        parent_id: r.4.map(|x| x.to_string()).unwrap_or_default(),
        is_active: r.5,
        created_at: r.6.to_rfc3339(),
        updated_at: r.7.to_rfc3339(),
    }
}
#[tonic::async_trait]
impl CategoryService for CategoryGrpc {
    async fn create_category(
        &self,
        q: Request<CategoryInput>,
    ) -> Result<Response<CategoryResponse>, Status> {
        let x = q.into_inner();
        if x.name.trim().is_empty() || x.slug.trim().is_empty() {
            return Err(errors::invalid("name and slug are required"));
        }
        let parent = if x.parent_id.is_empty() {
            None
        } else {
            Some(parse_uuid(&x.parent_id, "parent_id")?)
        };
        let r=sqlx::query_as::<_,(uuid::Uuid,String,String,String,Option<uuid::Uuid>,bool,chrono::DateTime<chrono::Utc>,chrono::DateTime<chrono::Utc>)>("INSERT INTO categories(name,slug,description,parent_id,is_active) VALUES($1,$2,$3,$4,$5) RETURNING id,name,slug,description,parent_id,is_active,created_at,updated_at").bind(x.name.trim()).bind(x.slug.trim()).bind(x.description).bind(parent).bind(x.is_active).fetch_one(&self.pool).await.map_err(errors::db)?;
        Ok(Response::new(CategoryResponse {
            category: Some(map(r)),
        }))
    }
    async fn get_category(
        &self,
        q: Request<CategoryId>,
    ) -> Result<Response<CategoryResponse>, Status> {
        let id = parse_uuid(&q.into_inner().id, "id")?;
        let r=sqlx::query_as::<_,(uuid::Uuid,String,String,String,Option<uuid::Uuid>,bool,chrono::DateTime<chrono::Utc>,chrono::DateTime<chrono::Utc>)>("SELECT id,name,slug,description,parent_id,is_active,created_at,updated_at FROM categories WHERE id=$1").bind(id).fetch_optional(&self.pool).await.map_err(errors::db)?.ok_or_else(||Status::not_found("category not found"))?;
        Ok(Response::new(CategoryResponse {
            category: Some(map(r)),
        }))
    }
    async fn list_categories(
        &self,
        q: Request<PageRequest>,
    ) -> Result<Response<CategoryList>, Status> {
        let x = q.into_inner();
        let lim = page_size(x.page_size);
        let off = page_offset(&x.page_token)?;
        let rows=sqlx::query_as::<_,(uuid::Uuid,String,String,String,Option<uuid::Uuid>,bool,chrono::DateTime<chrono::Utc>,chrono::DateTime<chrono::Utc>)>("SELECT id,name,slug,description,parent_id,is_active,created_at,updated_at FROM categories ORDER BY name LIMIT $1 OFFSET $2").bind(lim+1).bind(off).fetch_all(&self.pool).await.map_err(errors::db)?;
        let more = rows.len() as i64 > lim;
        let categories = rows.into_iter().take(lim as usize).map(map).collect();
        Ok(Response::new(CategoryList {
            categories,
            next_page_token: if more {
                (off + lim).to_string()
            } else {
                String::new()
            },
        }))
    }
    async fn update_category(
        &self,
        q: Request<CategoryInput>,
    ) -> Result<Response<CategoryResponse>, Status> {
        let x = q.into_inner();
        let id = parse_uuid(&x.id, "id")?;
        if x.name.trim().is_empty() || x.slug.trim().is_empty() {
            return Err(errors::invalid("name and slug are required"));
        }
        let parent = if x.parent_id.is_empty() {
            None
        } else {
            Some(parse_uuid(&x.parent_id, "parent_id")?)
        };
        if parent == Some(id) {
            return Err(errors::invalid("category cannot be its own parent"));
        }
        if let Some(parent_id) = parent {
            let cycle: bool = sqlx::query_scalar("WITH RECURSIVE ancestors(id,parent_id) AS (SELECT id,parent_id FROM categories WHERE id=$1 UNION ALL SELECT c.id,c.parent_id FROM categories c JOIN ancestors a ON c.id=a.parent_id) SELECT EXISTS(SELECT 1 FROM ancestors WHERE id=$2)")
                .bind(parent_id).bind(id).fetch_one(&self.pool).await.map_err(errors::db)?;
            if cycle {
                return Err(errors::invalid("category parent would create a cycle"));
            }
        }
        let r=sqlx::query_as::<_,(uuid::Uuid,String,String,String,Option<uuid::Uuid>,bool,chrono::DateTime<chrono::Utc>,chrono::DateTime<chrono::Utc>)>("UPDATE categories SET name=$2,slug=$3,description=$4,parent_id=$5,is_active=$6 WHERE id=$1 RETURNING id,name,slug,description,parent_id,is_active,created_at,updated_at").bind(id).bind(x.name.trim()).bind(x.slug.trim()).bind(x.description).bind(parent).bind(x.is_active).fetch_optional(&self.pool).await.map_err(errors::db)?.ok_or_else(||Status::not_found("category not found"))?;
        Ok(Response::new(CategoryResponse {
            category: Some(map(r)),
        }))
    }
    async fn delete_category(
        &self,
        q: Request<CategoryId>,
    ) -> Result<Response<OperationResponse>, Status> {
        let id = parse_uuid(&q.into_inner().id, "id")?;
        let mut tx = self.pool.begin().await.map_err(errors::db)?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM categories WHERE id=$1)",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await
        .map_err(errors::db)?;
        if !exists {
            return Err(Status::not_found("category not found"));
        }
        let used:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM products WHERE category_id=$1) OR EXISTS(SELECT 1 FROM categories WHERE parent_id=$1)").bind(id).fetch_one(&mut *tx).await.map_err(errors::db)?;
        if used {
            return Err(Status::failed_precondition(
                "category has products or children",
            ));
        }
        sqlx::query("DELETE FROM categories WHERE id=$1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(errors::db)?;
        tx.commit().await.map_err(errors::db)?;
        Ok(Response::new(OperationResponse { success: true }))
    }
}
