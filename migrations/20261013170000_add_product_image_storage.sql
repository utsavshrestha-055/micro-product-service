ALTER TABLE product_images ADD COLUMN image_data bytea;
ALTER TABLE product_images ADD COLUMN content_type text NOT NULL DEFAULT '';
