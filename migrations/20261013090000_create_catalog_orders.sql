CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.updated_at = now(); RETURN NEW; END $$;
CREATE TABLE categories (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), name text NOT NULL, slug text NOT NULL UNIQUE,
 description text NOT NULL DEFAULT '', parent_id uuid REFERENCES categories(id), is_active boolean NOT NULL DEFAULT true,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz,
 CHECK(parent_id IS NULL OR parent_id <> id)
);
CREATE INDEX categories_parent_idx ON categories(parent_id) WHERE deleted_at IS NULL;
CREATE TRIGGER categories_updated_at BEFORE UPDATE ON categories FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TABLE products (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), category_id uuid NOT NULL REFERENCES categories(id), name text NOT NULL,
 slug text NOT NULL UNIQUE, sku text NOT NULL UNIQUE, short_description text NOT NULL DEFAULT '', price_minor bigint NOT NULL CHECK(price_minor >= 0),
 currency char(3) NOT NULL, stock_quantity bigint NOT NULL DEFAULT 0 CHECK(stock_quantity >= 0), is_active boolean NOT NULL DEFAULT true,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz
);
CREATE INDEX products_category_idx ON products(category_id) WHERE deleted_at IS NULL;
CREATE INDEX products_active_price_idx ON products(is_active, price_minor) WHERE deleted_at IS NULL;
CREATE INDEX products_name_search_idx ON products USING gin(to_tsvector('simple', name)) WHERE deleted_at IS NULL;
CREATE TRIGGER products_updated_at BEFORE UPDATE ON products FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TABLE product_details (
 product_id uuid PRIMARY KEY REFERENCES products(id), long_description text NOT NULL DEFAULT '', specs jsonb NOT NULL DEFAULT '{}'::jsonb,
 brand text NOT NULL DEFAULT '', weight text NOT NULL DEFAULT '', dimensions jsonb NOT NULL DEFAULT '{}'::jsonb,
 seo_title text NOT NULL DEFAULT '', meta_description text NOT NULL DEFAULT '', tags text[] NOT NULL DEFAULT '{}',
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER product_details_updated_at BEFORE UPDATE ON product_details FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TABLE product_images (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), product_id uuid NOT NULL REFERENCES products(id), url text NOT NULL,
 alt_text text NOT NULL DEFAULT '', sort_order integer NOT NULL DEFAULT 0, is_primary boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX product_images_product_order_idx ON product_images(product_id, sort_order);
CREATE UNIQUE INDEX product_images_one_primary_idx ON product_images(product_id) WHERE is_primary;
CREATE TRIGGER product_images_updated_at BEFORE UPDATE ON product_images FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TYPE order_status AS ENUM ('PENDING','CONFIRMED','PAID','SHIPPED','DELIVERED','CANCELLED','REFUNDED');
CREATE TABLE orders (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), user_id uuid NOT NULL, status order_status NOT NULL DEFAULT 'PENDING',
 total_minor bigint NOT NULL CHECK(total_minor >= 0), currency char(3) NOT NULL, shipping_address jsonb NOT NULL,
 idempotency_key text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(user_id, idempotency_key)
);
CREATE INDEX orders_user_created_idx ON orders(user_id, created_at DESC);
CREATE INDEX orders_status_created_idx ON orders(status, created_at DESC);
CREATE TRIGGER orders_updated_at BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TABLE order_items (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), order_id uuid NOT NULL REFERENCES orders(id), product_id uuid NOT NULL,
 product_name text NOT NULL, sku text NOT NULL, unit_price_minor bigint NOT NULL CHECK(unit_price_minor >= 0),
 quantity bigint NOT NULL CHECK(quantity > 0), line_total_minor bigint NOT NULL CHECK(line_total_minor >= 0)
);
CREATE INDEX order_items_order_idx ON order_items(order_id);
CREATE INDEX order_items_product_idx ON order_items(product_id);
CREATE TABLE order_status_history (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), order_id uuid NOT NULL REFERENCES orders(id), from_status order_status,
 to_status order_status NOT NULL, changed_by text NOT NULL DEFAULT '', note text NOT NULL DEFAULT '', changed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX order_status_history_order_time_idx ON order_status_history(order_id, changed_at DESC);
