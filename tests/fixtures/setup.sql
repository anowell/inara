-- Test schema setup
-- Creates tables used by integration tests.
-- Run with: just db-test-setup

-- Clean slate
DROP SCHEMA IF EXISTS inara_test CASCADE;
CREATE SCHEMA inara_test;

-- ============================================================
-- Enum types
-- ============================================================
CREATE TYPE inara_test.status AS ENUM ('active', 'inactive', 'pending');
CREATE TYPE inara_test.priority AS ENUM ('low', 'medium', 'high', 'critical');

-- ============================================================
-- Composite type
-- ============================================================
CREATE TYPE inara_test.address AS (
    street TEXT,
    city   TEXT,
    zip    VARCHAR(10)
);

-- ============================================================
-- Domain type
-- ============================================================
CREATE DOMAIN inara_test.email AS TEXT
    CHECK (VALUE ~ '^.+@.+\..+$');

-- ============================================================
-- Tables
-- ============================================================

-- Users: basic table with PK, unique, check, defaults, enum column
CREATE TABLE inara_test.users (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email      inara_test.email NOT NULL,
    name       VARCHAR(255) NOT NULL,
    status     inara_test.status NOT NULL DEFAULT 'active',
    age        INTEGER,
    bio        TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT users_email_key UNIQUE (email),
    CONSTRAINT users_age_check CHECK (age > 0 AND age < 200)
);

-- Categories: self-referential FK
CREATE TABLE inara_test.categories (
    id        SERIAL PRIMARY KEY,
    name      TEXT NOT NULL,
    parent_id INTEGER,
    CONSTRAINT categories_parent_fk FOREIGN KEY (parent_id)
        REFERENCES inara_test.categories (id)
        ON DELETE SET NULL
);

-- Posts: FK to users, array columns, nullable fields
CREATE TABLE inara_test.posts (
    id         BIGSERIAL PRIMARY KEY,
    author_id  UUID NOT NULL,
    title      TEXT NOT NULL,
    body       TEXT,
    tags       TEXT[] DEFAULT '{}',
    priority   inara_test.priority NOT NULL DEFAULT 'medium',
    metadata   JSONB,
    score      NUMERIC(5, 2) DEFAULT 0.00,
    published  BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ,
    CONSTRAINT posts_author_fk FOREIGN KEY (author_id)
        REFERENCES inara_test.users (id)
        ON DELETE CASCADE
        ON UPDATE NO ACTION,
    CONSTRAINT posts_score_check CHECK (score >= 0)
);

-- Comments: FK to both users and posts, composite unique
CREATE TABLE inara_test.comments (
    id        BIGSERIAL PRIMARY KEY,
    post_id   BIGINT NOT NULL,
    author_id UUID NOT NULL,
    body      TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT comments_post_fk FOREIGN KEY (post_id)
        REFERENCES inara_test.posts (id)
        ON DELETE CASCADE,
    CONSTRAINT comments_author_fk FOREIGN KEY (author_id)
        REFERENCES inara_test.users (id)
        ON DELETE CASCADE
);

-- ============================================================
-- Indexes
-- ============================================================

-- Simple index
CREATE INDEX posts_author_idx ON inara_test.posts (author_id);

-- Composite index
CREATE INDEX posts_author_created_idx ON inara_test.posts (author_id, created_at);

-- Unique index
CREATE UNIQUE INDEX users_name_email_idx ON inara_test.users (name, email);

-- Partial index
CREATE INDEX posts_published_idx ON inara_test.posts (created_at)
    WHERE published = true;

-- Index on comments
CREATE INDEX comments_post_idx ON inara_test.comments (post_id);
