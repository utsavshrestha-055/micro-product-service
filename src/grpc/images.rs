use crate::{
    errors,
    grpc::{page_offset, page_size, parse_uuid},
    proto::{
        ImageId, OperationResponse, ProductImage, ProductImageDataResponse, ProductImageInput,
        ProductImageList, ProductImageListRequest, ProductImageResponse,
        product_image_service_server::ProductImageService,
    },
};
use sqlx::PgPool;
use tonic::{Request, Response, Status};
#[derive(Clone)]
pub struct ImageGrpc {
    pool: PgPool,
}
impl ImageGrpc {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
type Row = (
    uuid::Uuid,
    uuid::Uuid,
    String,
    String,
    i32,
    bool,
    chrono::DateTime<chrono::Utc>,
    chrono::DateTime<chrono::Utc>,
);
fn image(r: Row) -> ProductImage {
    ProductImage {
        id: r.0.to_string(),
        product_id: r.1.to_string(),
        url: r.2,
        alt_text: r.3,
        sort_order: r.4,
        is_primary: r.5,
        created_at: r.6.to_rfc3339(),
        updated_at: r.7.to_rfc3339(),
    }
}
#[tonic::async_trait]
impl ProductImageService for ImageGrpc {
    async fn create_product_image(
        &self,
        q: Request<ProductImageInput>,
    ) -> Result<Response<ProductImageResponse>, Status> {
        let x = q.into_inner();
        let p = parse_uuid(&x.product_id, "product_id")?;
        validate_image_source(&x.url, &x.image_data, &x.content_type)?;
        let mut tx = self.pool.begin().await.map_err(errors::db)?;
        let any: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM product_images WHERE product_id=$1)")
                .bind(p)
                .fetch_one(&mut *tx)
                .await
                .map_err(errors::db)?;
        let primary = x.is_primary || !any;
        if primary {
            sqlx::query("UPDATE product_images SET is_primary=false WHERE product_id=$1")
                .bind(p)
                .execute(&mut *tx)
                .await
                .map_err(errors::db)?;
        }
        let r=sqlx::query_as::<_,Row>("INSERT INTO product_images(product_id,url,alt_text,sort_order,is_primary,image_data,content_type) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id,product_id,url,alt_text,sort_order,is_primary,created_at,updated_at").bind(p).bind(x.url).bind(x.alt_text).bind(x.sort_order).bind(primary).bind(if x.image_data.is_empty() { None } else { Some(x.image_data) }).bind(x.content_type).fetch_one(&mut *tx).await.map_err(errors::db)?;
        tx.commit().await.map_err(errors::db)?;
        Ok(Response::new(ProductImageResponse {
            image: Some(image(r)),
        }))
    }
    async fn get_product_image(
        &self,
        q: Request<ImageId>,
    ) -> Result<Response<ProductImageResponse>, Status> {
        let id = parse_uuid(&q.into_inner().id, "id")?;
        let r=sqlx::query_as::<_,Row>("SELECT id,product_id,url,alt_text,sort_order,is_primary,created_at,updated_at FROM product_images WHERE id=$1").bind(id).fetch_optional(&self.pool).await.map_err(errors::db)?.ok_or_else(||Status::not_found("image not found"))?;
        Ok(Response::new(ProductImageResponse {
            image: Some(image(r)),
        }))
    }
    async fn list_product_images(
        &self,
        q: Request<ProductImageListRequest>,
    ) -> Result<Response<ProductImageList>, Status> {
        let x = q.into_inner();
        let p = parse_uuid(&x.product_id, "product_id")?;
        let limit = page_size(x.page_size);
        let offset = page_offset(&x.page_token)?;
        let rows=sqlx::query_as::<_,Row>("SELECT id,product_id,url,alt_text,sort_order,is_primary,created_at,updated_at FROM product_images WHERE product_id=$1 ORDER BY sort_order,id LIMIT $2 OFFSET $3").bind(p).bind(limit+1).bind(offset).fetch_all(&self.pool).await.map_err(errors::db)?;
        let more = rows.len() as i64 > limit;
        Ok(Response::new(ProductImageList {
            images: rows.into_iter().take(limit as usize).map(image).collect(),
            next_page_token: if more {
                (offset + limit).to_string()
            } else {
                String::new()
            },
        }))
    }
    async fn update_product_image(
        &self,
        q: Request<ProductImageInput>,
    ) -> Result<Response<ProductImageResponse>, Status> {
        let x = q.into_inner();
        let id = parse_uuid(&x.id, "id")?;
        let mut tx = self.pool.begin().await.map_err(errors::db)?;
        let p: (uuid::Uuid, bool) = sqlx::query_as(
            "SELECT product_id,is_primary FROM product_images WHERE id=$1 FOR UPDATE",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(errors::db)?
        .ok_or_else(|| Status::not_found("image not found"))?;
        if !x.image_data.is_empty() || !x.url.is_empty() {
            validate_image_source(&x.url, &x.image_data, &x.content_type)?;
        }
        if x.is_primary {
            sqlx::query("UPDATE product_images SET is_primary=false WHERE product_id=$1")
                .bind(p.0)
                .execute(&mut *tx)
                .await
                .map_err(errors::db)?;
        } else if p.1 {
            let other:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM product_images WHERE product_id=$1 AND id<>$2 AND is_primary)").bind(p.0).bind(id).fetch_one(&mut *tx).await.map_err(errors::db)?;
            if !other {
                return Err(Status::failed_precondition(
                    "product must retain a primary image",
                ));
            }
        }
        let r=sqlx::query_as::<_,Row>("UPDATE product_images SET url=CASE WHEN $2<>'' THEN $2 WHEN $6::bytea IS NOT NULL THEN '' ELSE url END,alt_text=$3,sort_order=$4,is_primary=$5,image_data=CASE WHEN $2<>'' THEN NULL ELSE COALESCE($6,image_data) END,content_type=CASE WHEN $2<>'' THEN '' WHEN $6::bytea IS NOT NULL THEN $7 ELSE content_type END WHERE id=$1 RETURNING id,product_id,url,alt_text,sort_order,is_primary,created_at,updated_at").bind(id).bind(x.url).bind(x.alt_text).bind(x.sort_order).bind(x.is_primary).bind(if x.image_data.is_empty() { None } else { Some(x.image_data) }).bind(x.content_type).fetch_one(&mut *tx).await.map_err(errors::db)?;
        tx.commit().await.map_err(errors::db)?;
        Ok(Response::new(ProductImageResponse {
            image: Some(image(r)),
        }))
    }
    async fn get_product_image_data(
        &self,
        q: Request<ImageId>,
    ) -> Result<Response<ProductImageDataResponse>, Status> {
        let id = parse_uuid(&q.into_inner().id, "id")?;
        let row = sqlx::query_as::<_, (Option<Vec<u8>>, String)>(
            "SELECT image_data,content_type FROM product_images WHERE id=$1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(errors::db)?
        .ok_or_else(|| Status::not_found("image not found"))?;
        let image_data = row
            .0
            .ok_or_else(|| Status::not_found("image file not found"))?;
        Ok(Response::new(ProductImageDataResponse {
            image_data,
            content_type: row.1,
        }))
    }

    async fn delete_product_image(
        &self,
        q: Request<ImageId>,
    ) -> Result<Response<OperationResponse>, Status> {
        let id = parse_uuid(&q.into_inner().id, "id")?;
        let mut tx = self.pool.begin().await.map_err(errors::db)?;
        let r = sqlx::query_as::<_, (uuid::Uuid, bool)>(
            "SELECT product_id,is_primary FROM product_images WHERE id=$1 FOR UPDATE",
        )
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(errors::db)?
        .ok_or_else(|| Status::not_found("image not found"))?;
        if r.1 {
            let other:Option<uuid::Uuid>=sqlx::query_scalar("SELECT id FROM product_images WHERE product_id=$1 AND id<>$2 ORDER BY sort_order,id LIMIT 1 FOR UPDATE").bind(r.0).bind(id).fetch_optional(&mut *tx).await.map_err(errors::db)?;
            if let Some(other) = other {
                sqlx::query("DELETE FROM product_images WHERE id=$1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(errors::db)?;
                sqlx::query("UPDATE product_images SET is_primary=true WHERE id=$1")
                    .bind(other)
                    .execute(&mut *tx)
                    .await
                    .map_err(errors::db)?;
            } else {
                return Err(Status::failed_precondition(
                    "cannot delete the only primary image",
                ));
            }
        } else {
            sqlx::query("DELETE FROM product_images WHERE id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(errors::db)?;
        }
        tx.commit().await.map_err(errors::db)?;
        Ok(Response::new(OperationResponse { success: true }))
    }
}

fn validate_image_source(url: &str, image_data: &[u8], content_type: &str) -> Result<(), Status> {
    let is_upload = !image_data.is_empty();
    let is_url = url.starts_with("https://") || url.starts_with("http://");
    if is_upload {
        if image_data.len() > 5 * 1024 * 1024 {
            return Err(errors::invalid("image data must not exceed 5 MiB"));
        }
        let matches_content_type = match content_type {
            "image/jpeg" => image_data.starts_with(&[0xff, 0xd8, 0xff]),
            "image/png" => {
                image_data.starts_with(&[0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a])
            }
            "image/webp" => {
                image_data.len() >= 12
                    && &image_data[..4] == b"RIFF"
                    && &image_data[8..12] == b"WEBP"
            }
            _ => false,
        };
        if !matches_content_type {
            return Err(errors::invalid(
                "image data does not match a supported content_type",
            ));
        }
    }
    if !is_upload && !is_url {
        return Err(errors::invalid(
            "provide image data or an absolute http(s) url",
        ));
    }
    Ok(())
}
