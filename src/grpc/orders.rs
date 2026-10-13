use crate::{
    errors,
    grpc::{page_offset, page_size, parse_uuid},
    proto::orders::{
        CancelOrderRequest, CreateOrderRequest, GetOrderAdminRequest, GetOrderRequest,
        ListOrdersAdminRequest, ListOrdersRequest, Order, OrderItem, OrderList, OrderResponse,
        OrderStatus, UpdateOrderStatusRequest, order_service_server::OrderService,
    },
};
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::BTreeMap;
use tonic::{Request, Response, Status};
#[derive(Clone)]
pub struct OrderGrpc {
    pool: PgPool,
}
impl OrderGrpc {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
fn st(s: &str) -> OrderStatus {
    OrderStatus::from_str_name(s).unwrap_or(OrderStatus::Pending)
}
fn status_text(s: OrderStatus) -> &'static str {
    match s {
        OrderStatus::Pending => "PENDING",
        OrderStatus::Confirmed => "CONFIRMED",
        OrderStatus::Paid => "PAID",
        OrderStatus::Shipped => "SHIPPED",
        OrderStatus::Delivered => "DELIVERED",
        OrderStatus::Cancelled => "CANCELLED",
        OrderStatus::Refunded => "REFUNDED",
    }
}
pub fn transition_allowed(from: OrderStatus, to: OrderStatus) -> bool {
    use OrderStatus::*;
    matches!(
        (from, to),
        (Pending, Confirmed)
            | (Pending, Cancelled)
            | (Confirmed, Paid)
            | (Confirmed, Cancelled)
            | (Paid, Shipped)
            | (Paid, Refunded)
            | (Shipped, Delivered)
            | (Delivered, Refunded)
    )
}
impl OrderGrpc {
    async fn fetch_order(&self, id: uuid::Uuid) -> Result<Order, Status> {
        let h=sqlx::query_as::<_,(uuid::Uuid,uuid::Uuid,String,i64,String,serde_json::Value,chrono::DateTime<chrono::Utc>,chrono::DateTime<chrono::Utc>)>("SELECT id,user_id,status::text,total_minor,currency,shipping_address,created_at,updated_at FROM orders WHERE id=$1").bind(id).fetch_optional(&self.pool).await.map_err(errors::db)?.ok_or_else(||Status::not_found("order not found"))?;
        let items=sqlx::query_as::<_,(uuid::Uuid,String,String,i64,i64,i64)>("SELECT product_id,product_name,sku,unit_price_minor,quantity,line_total_minor FROM order_items WHERE order_id=$1 ORDER BY id").bind(id).fetch_all(&self.pool).await.map_err(errors::db)?.into_iter().map(|r|OrderItem{product_id:r.0.to_string(),product_name:r.1,sku:r.2,unit_price_minor:r.3,quantity:r.4,line_total_minor:r.5}).collect();
        Ok(Order {
            id: h.0.to_string(),
            user_id: h.1.to_string(),
            status: st(&h.2).into(),
            total_minor: h.3,
            currency: h.4.trim().into(),
            shipping_address_json: h.5.to_string(),
            items,
            created_at: h.6.to_rfc3339(),
            updated_at: h.7.to_rfc3339(),
        })
    }
    async fn fetch_user_order(&self, id: uuid::Uuid, user: uuid::Uuid) -> Result<Order, Status> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM orders WHERE id=$1 AND user_id=$2)")
                .bind(id)
                .bind(user)
                .fetch_one(&self.pool)
                .await
                .map_err(errors::db)?;
        if !exists {
            return Err(Status::not_found("order not found"));
        }
        let mut o = self.fetch_order(id).await?;
        o.user_id = user.to_string();
        Ok(o)
    }
    async fn list(
        &self,
        user: Option<uuid::Uuid>,
        status: Option<OrderStatus>,
        after: Option<chrono::DateTime<chrono::Utc>>,
        before: Option<chrono::DateTime<chrono::Utc>>,
        lim: i64,
        off: i64,
    ) -> Result<(Vec<Order>, bool), Status> {
        let rows=sqlx::query_as::<_,(uuid::Uuid,uuid::Uuid,String,i64,String,serde_json::Value,chrono::DateTime<chrono::Utc>,chrono::DateTime<chrono::Utc>)>("SELECT id,user_id,status::text,total_minor,currency,shipping_address,created_at,updated_at FROM orders WHERE ($1::uuid IS NULL OR user_id=$1) AND ($2::text IS NULL OR status::text=$2) AND ($3::timestamptz IS NULL OR created_at >= $3) AND ($4::timestamptz IS NULL OR created_at <= $4) ORDER BY created_at DESC,id LIMIT $5 OFFSET $6").bind(user).bind(status.map(|s|status_text(s).to_string())).bind(after).bind(before).bind(lim+1).bind(off).fetch_all(&self.pool).await.map_err(errors::db)?;
        let more = rows.len() as i64 > lim;
        let mut result = Vec::new();
        for r in rows.into_iter().take(lim as usize) {
            let items=sqlx::query_as::<_,(uuid::Uuid,String,String,i64,i64,i64)>("SELECT product_id,product_name,sku,unit_price_minor,quantity,line_total_minor FROM order_items WHERE order_id=$1 ORDER BY id").bind(r.0).fetch_all(&self.pool).await.map_err(errors::db)?.into_iter().map(|i|OrderItem{product_id:i.0.to_string(),product_name:i.1,sku:i.2,unit_price_minor:i.3,quantity:i.4,line_total_minor:i.5}).collect();
            result.push(Order {
                id: r.0.to_string(),
                user_id: r.1.to_string(),
                status: st(&r.2).into(),
                total_minor: r.3,
                currency: r.4.trim().into(),
                shipping_address_json: r.5.to_string(),
                items,
                created_at: r.6.to_rfc3339(),
                updated_at: r.7.to_rfc3339(),
            });
        }
        Ok((result, more))
    }
    async fn change_status(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        id: uuid::Uuid,
        to: OrderStatus,
        by: &str,
        note: &str,
    ) -> Result<(), Status> {
        let from_s: Option<String> =
            sqlx::query_scalar("SELECT status::text FROM orders WHERE id=$1 FOR UPDATE")
                .bind(id)
                .fetch_optional(&mut **tx)
                .await
                .map_err(errors::db)?;
        let from = from_s.ok_or_else(|| Status::not_found("order not found"))?;
        let f = st(&from);
        if !transition_allowed(f, to) {
            return Err(Status::failed_precondition(format!(
                "invalid order status transition: {from} -> {}",
                status_text(to)
            )));
        }
        if to == OrderStatus::Cancelled
            && matches!(f, OrderStatus::Pending | OrderStatus::Confirmed)
        {
            let rows = sqlx::query_as::<_, (uuid::Uuid, i64)>(
                "SELECT product_id,quantity FROM order_items WHERE order_id=$1",
            )
            .bind(id)
            .fetch_all(&mut **tx)
            .await
            .map_err(errors::db)?;
            for (pid, n) in rows {
                sqlx::query("UPDATE products SET stock_quantity=stock_quantity+$2 WHERE id=$1")
                    .bind(pid)
                    .bind(n)
                    .execute(&mut **tx)
                    .await
                    .map_err(errors::db)?;
            }
        }
        sqlx::query("UPDATE orders SET status=$2::order_status WHERE id=$1")
            .bind(id)
            .bind(status_text(to))
            .execute(&mut **tx)
            .await
            .map_err(errors::db)?;
        sqlx::query("INSERT INTO order_status_history(order_id,from_status,to_status,changed_by,note) VALUES($1,$2::order_status,$3::order_status,$4,$5)").bind(id).bind(from).bind(status_text(to)).bind(by).bind(note).execute(&mut **tx).await.map_err(errors::db)?;
        Ok(())
    }
}
#[tonic::async_trait]
impl OrderService for OrderGrpc {
    async fn create_order(
        &self,
        q: Request<CreateOrderRequest>,
    ) -> Result<Response<OrderResponse>, Status> {
        let x = q.into_inner();
        let user = parse_uuid(&x.user_id, "user_id")?;
        if x.idempotency_key.trim().is_empty() {
            return Err(errors::invalid("idempotency_key is required"));
        }
        if x.items.is_empty() {
            return Err(errors::invalid("at least one item is required"));
        }
        if x.currency.len() != 3 {
            return Err(errors::invalid("currency must be a three-letter code"));
        }
        let address: serde_json::Value = serde_json::from_str(&x.shipping_address_json)
            .map_err(|_| errors::invalid("shipping_address_json must be valid JSON"))?;
        if !address.is_object() {
            return Err(errors::invalid("shipping address must be a JSON object"));
        }
        let mut quantities = BTreeMap::<uuid::Uuid, i64>::new();
        for i in x.items {
            let id = parse_uuid(&i.product_id, "product_id")?;
            if i.quantity <= 0 {
                return Err(errors::invalid("quantity must be positive"));
            }
            let n = quantities.entry(id).or_default();
            *n = n
                .checked_add(i.quantity)
                .ok_or_else(|| errors::invalid("quantity overflow"))?;
        }
        let mut tx = self.pool.begin().await.map_err(errors::db)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!("{}:{}", user, x.idempotency_key))
            .execute(&mut *tx)
            .await
            .map_err(errors::db)?;
        let prior: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT id FROM orders WHERE user_id=$1 AND idempotency_key=$2")
                .bind(user)
                .bind(&x.idempotency_key)
                .fetch_optional(&mut *tx)
                .await
                .map_err(errors::db)?;
        if let Some(id) = prior {
            tx.rollback().await.ok();
            return Ok(Response::new(OrderResponse {
                order: Some(self.fetch_user_order(id, user).await?),
            }));
        }
        let ids: Vec<uuid::Uuid> = quantities.keys().copied().collect();
        let rows=sqlx::query_as::<_,(uuid::Uuid,String,String,i64,String,i64,bool)>("SELECT id,name,sku,price_minor,currency,stock_quantity,is_active FROM products WHERE id=ANY($1) AND deleted_at IS NULL ORDER BY id FOR UPDATE").bind(&ids).fetch_all(&mut *tx).await.map_err(errors::db)?;
        if rows.len() != ids.len() {
            return Err(Status::not_found("one or more products not found"));
        }
        let mut total = 0i64;
        for r in &rows {
            let n = quantities[&r.0];
            if !r.6 {
                return Err(Status::failed_precondition(format!(
                    "product {} is inactive",
                    r.0
                )));
            }
            if r.5 < n {
                return Err(Status::failed_precondition(format!(
                    "insufficient stock for product {}",
                    r.0
                )));
            }
            if !r.4.trim().eq_ignore_ascii_case(&x.currency) {
                return Err(Status::failed_precondition(
                    "all items must use the requested currency",
                ));
            }
            total = total
                .checked_add(
                    r.3.checked_mul(n)
                        .ok_or_else(|| Status::failed_precondition("order total overflow"))?,
                )
                .ok_or_else(|| Status::failed_precondition("order total overflow"))?;
        }
        let id:uuid::Uuid=sqlx::query_scalar("INSERT INTO orders(user_id,status,total_minor,currency,shipping_address,idempotency_key) VALUES($1,'PENDING',$2,upper($3),$4,$5) RETURNING id").bind(user).bind(total).bind(&x.currency).bind(address).bind(&x.idempotency_key).fetch_one(&mut *tx).await.map_err(errors::db)?;
        for r in rows {
            let n = quantities[&r.0];
            sqlx::query("UPDATE products SET stock_quantity=stock_quantity-$2 WHERE id=$1")
                .bind(r.0)
                .bind(n)
                .execute(&mut *tx)
                .await
                .map_err(errors::db)?;
            sqlx::query("INSERT INTO order_items(order_id,product_id,product_name,sku,unit_price_minor,quantity,line_total_minor) VALUES($1,$2,$3,$4,$5,$6,$7)").bind(id).bind(r.0).bind(r.1).bind(r.2).bind(r.3).bind(n).bind(r.3*n).execute(&mut *tx).await.map_err(errors::db)?;
        }
        sqlx::query("INSERT INTO order_status_history(order_id,from_status,to_status,changed_by,note) VALUES($1,NULL,'PENDING',$2,'order created')").bind(id).bind(user.to_string()).execute(&mut *tx).await.map_err(errors::db)?;
        tx.commit().await.map_err(errors::db)?;
        Ok(Response::new(OrderResponse {
            order: Some(self.fetch_user_order(id, user).await?),
        }))
    }
    async fn get_order(
        &self,
        q: Request<GetOrderRequest>,
    ) -> Result<Response<OrderResponse>, Status> {
        let x = q.into_inner();
        let id = parse_uuid(&x.id, "id")?;
        let user = parse_uuid(&x.user_id, "user_id")?;
        Ok(Response::new(OrderResponse {
            order: Some(self.fetch_user_order(id, user).await?),
        }))
    }
    async fn list_orders(
        &self,
        q: Request<ListOrdersRequest>,
    ) -> Result<Response<OrderList>, Status> {
        let x = q.into_inner();
        let user = parse_uuid(&x.user_id, "user_id")?;
        let lim = page_size(x.page_size);
        let off = page_offset(&x.page_token)?;
        let status = x
            .status
            .map(|n| OrderStatus::try_from(n).map_err(|_| errors::invalid("unknown status")))
            .transpose()?;
        let (orders, more) = self.list(Some(user), status, None, None, lim, off).await?;
        Ok(Response::new(OrderList {
            orders,
            next_page_token: if more {
                (off + lim).to_string()
            } else {
                String::new()
            },
        }))
    }
    async fn cancel_order(
        &self,
        q: Request<CancelOrderRequest>,
    ) -> Result<Response<OrderResponse>, Status> {
        let x = q.into_inner();
        let id = parse_uuid(&x.id, "id")?;
        let user = parse_uuid(&x.user_id, "user_id")?;
        let mut tx = self.pool.begin().await.map_err(errors::db)?;
        let owns: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM orders WHERE id=$1 AND user_id=$2)")
                .bind(id)
                .bind(user)
                .fetch_one(&mut *tx)
                .await
                .map_err(errors::db)?;
        if !owns {
            return Err(Status::not_found("order not found"));
        }
        self.change_status(
            &mut tx,
            id,
            OrderStatus::Cancelled,
            &user.to_string(),
            &x.note,
        )
        .await?;
        tx.commit().await.map_err(errors::db)?;
        Ok(Response::new(OrderResponse {
            order: Some(self.fetch_user_order(id, user).await?),
        }))
    }
    async fn get_order_admin(
        &self,
        q: Request<GetOrderAdminRequest>,
    ) -> Result<Response<OrderResponse>, Status> {
        let id = parse_uuid(&q.into_inner().id, "id")?;
        Ok(Response::new(OrderResponse {
            order: Some(self.fetch_order(id).await?),
        }))
    }
    async fn list_orders_admin(
        &self,
        q: Request<ListOrdersAdminRequest>,
    ) -> Result<Response<OrderList>, Status> {
        let x = q.into_inner();
        let user = if x.user_id.is_empty() {
            None
        } else {
            Some(parse_uuid(&x.user_id, "user_id")?)
        };
        let after = if x.created_after.is_empty() {
            None
        } else {
            Some(
                chrono::DateTime::parse_from_rfc3339(&x.created_after)
                    .map_err(|_| errors::invalid("created_after must be RFC3339"))?
                    .with_timezone(&chrono::Utc),
            )
        };
        let before = if x.created_before.is_empty() {
            None
        } else {
            Some(
                chrono::DateTime::parse_from_rfc3339(&x.created_before)
                    .map_err(|_| errors::invalid("created_before must be RFC3339"))?
                    .with_timezone(&chrono::Utc),
            )
        };
        if matches!((after,before),(Some(a),Some(b))if a>b) {
            return Err(errors::invalid(
                "created_after must not exceed created_before",
            ));
        }
        let status = x
            .status
            .map(|n| OrderStatus::try_from(n).map_err(|_| errors::invalid("unknown status")))
            .transpose()?;
        let lim = page_size(x.page_size);
        let off = page_offset(&x.page_token)?;
        let (orders, more) = self.list(user, status, after, before, lim, off).await?;
        Ok(Response::new(OrderList {
            orders,
            next_page_token: if more {
                (off + lim).to_string()
            } else {
                String::new()
            },
        }))
    }
    async fn update_order_status(
        &self,
        q: Request<UpdateOrderStatusRequest>,
    ) -> Result<Response<OrderResponse>, Status> {
        let x = q.into_inner();
        let id = parse_uuid(&x.id, "id")?;
        if x.changed_by.trim().is_empty() {
            return Err(errors::invalid("changed_by is required"));
        }
        let status =
            OrderStatus::try_from(x.status).map_err(|_| errors::invalid("unknown status"))?;
        let mut tx = self.pool.begin().await.map_err(errors::db)?;
        self.change_status(&mut tx, id, status, &x.changed_by, &x.note)
            .await?;
        tx.commit().await.map_err(errors::db)?;
        Ok(Response::new(OrderResponse {
            order: Some(self.fetch_order(id).await?),
        }))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn status_transitions_are_restricted() {
        assert!(transition_allowed(
            OrderStatus::Pending,
            OrderStatus::Confirmed
        ));
        assert!(transition_allowed(
            OrderStatus::Pending,
            OrderStatus::Cancelled
        ));
        assert!(transition_allowed(
            OrderStatus::Shipped,
            OrderStatus::Delivered
        ));
        assert!(!transition_allowed(
            OrderStatus::Shipped,
            OrderStatus::Cancelled
        ));
        assert!(!transition_allowed(
            OrderStatus::Delivered,
            OrderStatus::Paid
        ));
    }
    #[test]
    fn checked_order_math_rejects_overflow() {
        assert_eq!(100i64.checked_mul(3), Some(300));
        assert_eq!(i64::MAX.checked_mul(2), None);
        assert_eq!(i64::MAX.checked_add(1), None);
    }
}
