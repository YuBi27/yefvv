#!/bin/bash
# migrate.sh — Apply all pending DB migrations to the running postgres container
# Usage: ./migrate.sh

set -e

CONTAINER="test_bot-postgres-1"
DB_USER="bot"
DB_NAME="botdb"

echo "🔄 Running migrations on $CONTAINER..."

docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" << 'SQL'

-- ============================================================
-- Migration 001: Add section column to questions (if missing)
-- ============================================================
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='questions' AND column_name='section'
  ) THEN
    ALTER TABLE questions ADD COLUMN section VARCHAR DEFAULT '';
    RAISE NOTICE 'questions.section added';
  ELSE
    RAISE NOTICE 'questions.section already exists';
  END IF;
END $$;

-- ============================================================
-- Migration 002: Add progress tracking columns to user_results
-- ============================================================
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='user_results' AND column_name='section'
  ) THEN
    ALTER TABLE user_results ADD COLUMN section VARCHAR DEFAULT '';
    RAISE NOTICE 'user_results.section added';
  ELSE
    RAISE NOTICE 'user_results.section already exists';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='user_results' AND column_name='stopped_at'
  ) THEN
    ALTER TABLE user_results ADD COLUMN stopped_at INTEGER DEFAULT 0;
    RAISE NOTICE 'user_results.stopped_at added';
  ELSE
    RAISE NOTICE 'user_results.stopped_at already exists';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='user_results' AND column_name='completed'
  ) THEN
    ALTER TABLE user_results ADD COLUMN completed BOOLEAN DEFAULT TRUE;
    -- Mark all existing rows as completed
    UPDATE user_results SET completed = TRUE WHERE completed IS NULL;
    RAISE NOTICE 'user_results.completed added';
  ELSE
    RAISE NOTICE 'user_results.completed already exists';
  END IF;
END $$;

-- ============================================================
-- Migration 003: Add new profile columns to access_requests
-- ============================================================
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='access_requests' AND column_name='study_place'
  ) THEN
    ALTER TABLE access_requests ADD COLUMN study_place VARCHAR DEFAULT '';
    RAISE NOTICE 'access_requests.study_place added';
  ELSE
    RAISE NOTICE 'access_requests.study_place already exists';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='access_requests' AND column_name='course'
  ) THEN
    ALTER TABLE access_requests ADD COLUMN course VARCHAR DEFAULT '';
    RAISE NOTICE 'access_requests.course added';
  ELSE
    RAISE NOTICE 'access_requests.course already exists';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='access_requests' AND column_name='instagram'
  ) THEN
    ALTER TABLE access_requests ADD COLUMN instagram VARCHAR DEFAULT '';
    RAISE NOTICE 'access_requests.instagram added';
  ELSE
    RAISE NOTICE 'access_requests.instagram already exists';
  END IF;
END $$;

-- ============================================================
-- Summary
-- ============================================================
SELECT
  table_name,
  column_name,
  data_type,
  column_default
FROM information_schema.columns
WHERE table_name IN ('questions', 'user_results', 'access_requests')
  AND column_name IN (
    'section', 'stopped_at', 'completed',
    'study_place', 'course', 'instagram'
  )
ORDER BY table_name, column_name;

SQL

echo ""
echo "✅ Migration complete!"
