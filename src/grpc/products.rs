use crate::{
    errors,
    grpc::{page_offset, page_size, parse_uuid},
    proto::{
        Category, OperationResponse, Product, ProductDetails, ProductDetailsInput,
        ProductDetailsResponse, ProductId, ProductImage, ProductInput, ProductList,
        ProductListRequest, ProductPageResponse, ProductResponse,
        product_service_server::ProductService,
    },
};
use sqlx::PgPool;
use tonic::{Request, Response, Status};
#[derive(Clone)]
pub struct ProductGrpc {
    pool: PgPool,
}
impl ProductGrpc {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
type Row = (
    uuid::Uuid,
    uuid::Uuid,
    String,
    String,
    String,
    String,
    i64,
    String,
    i64,
    bool,
    chrono::DateTime<chrono::Utc>,
    chrono::DateTime<chrono::Utc>,
);
fn product(r: Row) -> Product {
    Product {
        id: r.0.to_string(),
        category_id: r.1.to_string(),
        name: r.2,
        slug: r.3,
        sku: r.4,
        short_description: r.5,
        price_minor: r.6,
        currency: r.7.trim().to_string(),
        stock_quantity: r.8,
        is_active: r.9,
        created_at: r.10.to_rfc3339(),
        updated_at: r.11.to_rfc3339(),
    }
}
fn validate(x: &ProductInput) -> Result<uuid::Uuid, Status> {
    if x.name.trim().is_empty() || x.slug.trim().is_empty() || x.sku.trim().is_empty() {
        return Err(errors::invalid("name, slug, and sku are required"));
    }
    if x.price_minor < 0 || x.stock_quantity < 0 {
        return Err(errors::invalid("price and stock must be non-negative"));
    }
    if x.currency.len() != 3 {
        return Err(errors::invalid("currency must be a three-letter code"));
    }
    parse_uuid(&x.category_id, "category_id")
}
#[tonic::async_trait]
impl ProductService for ProductGrpc {
    async fn create_product(
        &self,
        q: Request<ProductInput>,
    ) -> Result<Response<ProductResponse>, Status> {
        let x = q.into_inner();
        let c = validate(&x)?;
        let r=sqlx::query_as::<_,Row>("INSERT INTO products(category_id,name,slug,sku,short_description,price_minor,currency,stock_quantity,is_active) SELECT $1,$2,$3,$4,$5,$6,upper($7),$8,$9 WHERE EXISTS(SELECT 1 FROM categories WHERE id=$1 AND deleted_at IS NULL) RETURNING id,category_id,name,slug,sku,short_description,price_minor,currency,stock_quantity,is_active,created_at,updated_at").bind(c).bind(x.name.trim()).bind(x.slug.trim()).bind(x.sku.trim()).bind(x.short_description).bind(x.price_minor).bind(x.currency).bind(x.stock_quantity).bind(x.is_active).fetch_optional(&self.pool).await.map_err(errors::db)?.ok_or_else(||Status::failed_precondition("category is missing or deleted"))?;
        Ok(Response::new(ProductResponse {
            product: Some(product(r)),
        }))
    }
    async fn get_product(
        &self,
        q: Request<ProductId>,
    ) -> Result<Response<ProductResponse>, Status> {
        let id = parse_uuid(&q.into_inner().id, "id")?;
        let r=sqlx::query_as::<_,Row>("SELECT id,category_id,name,slug,sku,short_description,price_minor,currency,stock_quantity,is_active,created_at,updated_at FROM products WHERE id=$1 AND deleted_at IS NULL").bind(id).fetch_optional(&self.pool).await.map_err(errors::db)?.ok_or_else(||Status::not_found("product not found"))?;
        Ok(Response::new(ProductResponse {
            product: Some(product(r)),
        }))
    }
    async fn list_products(
        &self,
        q: Request<ProductListRequest>,
    ) -> Result<Response<ProductList>, Status> {
        let x = q.into_inner();
        let lim = page_size(x.page_size);
        let off = page_offset(&x.page_token)?;
        let cat = if x.category_id.is_empty() {
            None
        } else {
            Some(parse_uuid(&x.category_id, "category_id")?)
        };
        if x.min_price_minor.is_some_and(|v| v < 0)
            || x.max_price_minor.is_some_and(|v| v < 0)
            || matches!((x.min_price_minor,x.max_price_minor),(Some(a),Some(b))if a>b)
        {
            return Err(errors::invalid("invalid price range"));
        }
        let rows=sqlx::query_as::<_,Row>("SELECT id,category_id,name,slug,sku,short_description,price_minor,currency,stock_quantity,is_active,created_at,updated_at FROM products WHERE deleted_at IS NULL AND ($1::uuid IS NULL OR category_id=$1) AND ($2::bool IS NULL OR is_active=$2) AND ($3::bigint IS NULL OR price_minor >= $3) AND ($4::bigint IS NULL OR price_minor <= $4) AND ($5='' OR name ILIKE '%' || $5 || '%') ORDER BY created_at DESC,id LIMIT $6 OFFSET $7").bind(cat).bind(x.is_active).bind(x.min_price_minor).bind(x.max_price_minor).bind(x.name_search).bind(lim+1).bind(off).fetch_all(&self.pool).await.map_err(errors::db)?;
        let more = rows.len() as i64 > lim;
        Ok(Response::new(ProductList {
            products: rows.into_iter().take(lim as usize).map(product).collect(),
            next_page_token: if more {
                (off + lim).to_string()
            } else {
                String::new()
            },
        }))
    }
    async fn update_product(
        &self,
        q: Request<ProductInput>,
    ) -> Result<Response<ProductResponse>, Status> {
        let x = q.into_inner();
        let id = parse_uuid(&x.id, "id")?;
        let c = validate(&x)?;
        let r=sqlx::query_as::<_,Row>("UPDATE products SET category_id=$2,name=$3,slug=$4,sku=$5,short_description=$6,price_minor=$7,currency=upper($8),stock_quantity=$9,is_active=$10 WHERE id=$1 AND deleted_at IS NULL RETURNING id,category_id,name,slug,sku,short_description,price_minor,currency,stock_quantity,is_active,created_at,updated_at").bind(id).bind(c).bind(x.name.trim()).bind(x.slug.trim()).bind(x.sku.trim()).bind(x.short_description).bind(x.price_minor).bind(x.currency).bind(x.stock_quantity).bind(x.is_active).fetch_optional(&self.pool).await.map_err(errors::db)?.ok_or_else(||Status::not_found("product not found"))?;
        Ok(Response::new(ProductResponse {
            product: Some(product(r)),
        }))
    }
    async fn delete_product(
        &self,
        q: Request<ProductId>,
    ) -> Result<Response<OperationResponse>, Status> {
        let id = parse_uuid(&q.into_inner().id, "id")?;
        let r=sqlx::query("UPDATE products SET deleted_at=now(),is_active=false WHERE id=$1 AND deleted_at IS NULL").bind(id).execute(&self.pool).await.map_err(errors::db)?;
        if r.rows_affected() == 0 {
            return Err(Status::not_found("product not found"));
        }
        Ok(Response::new(OperationResponse { success: true }))
    }
    async fn create_product_details(
        &self,
        q: Request<ProductDetailsInput>,
    ) -> Result<Response<ProductDetailsResponse>, Status> {
        let x = q.into_inner();
        let id = parse_uuid(&x.product_id, "product_id")?;
        let specs: serde_json::Value = serde_json::from_str(if x.specs_json.is_empty() {
            "{}"
        } else {
            &x.specs_json
        })
        .map_err(|_| errors::invalid("specs_json must be JSON"))?;
        let dims: serde_json::Value = serde_json::from_str(if x.dimensions_json.is_empty() {
            "{}"
        } else {
            &x.dimensions_json
        })
        .map_err(|_| errors::invalid("dimensions_json must be JSON"))?;
        sqlx::query("INSERT INTO product_details(product_id,long_description,specs,brand,weight,dimensions,seo_title,meta_description,tags) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)").bind(id).bind(&x.long_description).bind(specs).bind(&x.brand).bind(&x.weight).bind(dims).bind(&x.seo_title).bind(&x.meta_description).bind(&x.tags).execute(&self.pool).await.map_err(errors::db)?;
        self.get_product_details(Request::new(ProductId { id: id.to_string() }))
            .await
    }
    async fn get_product_details(
        &self,
        q: Request<ProductId>,
    ) -> Result<Response<ProductDetailsResponse>, Status> {
        let id = parse_uuid(&q.into_inner().id, "product_id")?;
        let r=sqlx::query_as::<_,(String,serde_json::Value,String,String,serde_json::Value,String,String,Vec<String>)>("SELECT long_description,specs,brand,weight,dimensions,seo_title,meta_description,tags FROM product_details WHERE product_id=$1").bind(id).fetch_optional(&self.pool).await.map_err(errors::db)?.ok_or_else(||Status::not_found("product details not found"))?;
        Ok(Response::new(ProductDetailsResponse {
            details: Some(ProductDetails {
                product_id: id.to_string(),
                long_description: r.0,
                specs_json: r.1.to_string(),
                brand: r.2,
                weight: r.3,
                dimensions_json: r.4.to_string(),
                seo_title: r.5,
                meta_description: r.6,
                tags: r.7,
            }),
        }))
    }
    async fn update_product_details(
        &self,
        q: Request<ProductDetailsInput>,
    ) -> Result<Response<ProductDetailsResponse>, Status> {
        let x = q.into_inner();
        let id = parse_uuid(&x.product_id, "product_id")?;
        let specs: serde_json::Value = serde_json::from_str(if x.specs_json.is_empty() {
            "{}"
        } else {
            &x.specs_json
        })
        .map_err(|_| errors::invalid("specs_json must be JSON"))?;
        let dims: serde_json::Value = serde_json::from_str(if x.dimensions_json.is_empty() {
            "{}"
        } else {
            &x.dimensions_json
        })
        .map_err(|_| errors::invalid("dimensions_json must be JSON"))?;
        let r=sqlx::query("UPDATE product_details SET long_description=$2,specs=$3,brand=$4,weight=$5,dimensions=$6,seo_title=$7,meta_description=$8,tags=$9 WHERE product_id=$1").bind(id).bind(&x.long_description).bind(specs).bind(&x.brand).bind(&x.weight).bind(dims).bind(&x.seo_title).bind(&x.meta_description).bind(&x.tags).execute(&self.pool).await.map_err(errors::db)?;
        if r.rows_affected() == 0 {
            return Err(Status::not_found("product details not found"));
        }
        self.get_product_details(Request::new(ProductId { id: id.to_string() }))
            .await
    }
    async fn delete_product_details(
        &self,
        q: Request<ProductId>,
    ) -> Result<Response<OperationResponse>, Status> {
        let id = parse_uuid(&q.into_inner().id, "product_id")?;
        let r = sqlx::query("DELETE FROM product_details WHERE product_id=$1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(errors::db)?;
        if r.rows_affected() == 0 {
            return Err(Status::not_found("product details not found"));
        }
        Ok(Response::new(OperationResponse { success: true }))
    }
    async fn get_product_page(
        &self,
        q: Request<ProductId>,
    ) -> Result<Response<ProductPageResponse>, Status> {
        let id = parse_uuid(&q.into_inner().id, "id")?;
        let p = self
            .get_product(Request::new(ProductId { id: id.to_string() }))
            .await?
            .into_inner()
            .product
            .ok_or_else(|| Status::not_found("product not found"))?;
        let d = self
            .get_product_details(Request::new(ProductId { id: id.to_string() }))
            .await
            .ok()
            .and_then(|r| r.into_inner().details);
        let imgs=sqlx::query_as::<_,(uuid::Uuid,uuid::Uuid,String,String,i32,bool,chrono::DateTime<chrono::Utc>,chrono::DateTime<chrono::Utc>)>("SELECT id,product_id,url,alt_text,sort_order,is_primary,created_at,updated_at FROM product_images WHERE product_id=$1 ORDER BY sort_order,id").bind(id).fetch_all(&self.pool).await.map_err(errors::db)?.into_iter().map(|r|ProductImage{id:r.0.to_string(),product_id:r.1.to_string(),url:r.2,alt_text:r.3,sort_order:r.4,is_primary:r.5,created_at:r.6.to_rfc3339(),updated_at:r.7.to_rfc3339()}).collect();
        let mut path = Vec::new();
        let mut cid = Some(parse_uuid(&p.category_id, "category_id")?);
        for _ in 0..64 {
            let Some(cur) = cid else { break };
            let row=sqlx::query_as::<_,(uuid::Uuid,String,String,String,Option<uuid::Uuid>,bool,chrono::DateTime<chrono::Utc>,chrono::DateTime<chrono::Utc>)>("SELECT id,name,slug,description,parent_id,is_active,created_at,updated_at FROM categories WHERE id=$1 AND deleted_at IS NULL").bind(cur).fetch_optional(&self.pool).await.map_err(errors::db)?;
            let Some(row) = row else { break };
            cid = row.4;
            path.push(Category {
                id: row.0.to_string(),
                name: row.1,
                slug: row.2,
                description: row.3,
                parent_id: row.4.map(|v| v.to_string()).unwrap_or_default(),
                is_active: row.5,
                created_at: row.6.to_rfc3339(),
                updated_at: row.7.to_rfc3339(),
            });
        }
        path.reverse();
        Ok(Response::new(ProductPageResponse {
            product: Some(p),
            details: d,
            category_parent_path: path,
            images: imgs,
        }))
    }
}
