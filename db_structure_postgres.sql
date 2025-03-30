CREATE TABLE "post" (
  "id" varchar PRIMARY KEY,
  "author_fullname" varchar,
  "created_utc" timestamp,
  "selftext" varchar,
  "archived" bool,
  "downs" integer,
  "ups" integer,
  "edited" bool,
  "hidden" bool,
  "locked" bool,
  "num_comments" integer,
  "num_crossposts" integer,
  "permalink" varchar,
  "score" integer,
  "send_replies" bool,
  "spoiler" bool,
  "subreddit" varchar,
  "thumbnail" varchar,
  "thumbnail_height" integer,
  "thumbnail_width" integer,
  "title" varchar,
  "total_awards_received" integer,
  "top_awarded_type" varchar,
  "pinned" bool,
  "contest_mode" bool
);

CREATE TABLE "author" (
  "author_fullname" varchar PRIMARY KEY,
  "author" varchar,
  "author_premium" bool,
  "over_18" bool
);

CREATE TABLE "subreddit" (
  "id" varchar PRIMARY KEY,
  "name" varchar,
  "display_name" varchar,
  "display_name_prefixed" varchar,
  "title" varchar,
  "description" varchar,
  "public_description" varchar,
  "url" varchar,
  "subreddit_type" varchar,
  "lang" varchar,
  "created_utc" timestamp,
  "subscribers" integer,
  "over18" bool,
  "allow_discovery" bool,
  "allow_galleries" bool,
  "allow_images" bool,
  "allow_polls" bool,
  "allow_videogifs" bool,
  "allow_videos" bool,
  "allow_predictions" bool,
  "allow_predictions_tournament" bool,
  "allow_talks" bool,
  "is_crosspostable_subreddit" bool,
  "community_reviewed" bool,
  "hide_ads" bool,
  "restrict_commenting" bool,
  "restrict_posting" bool,
  "quarantine" bool,
  "spoilers_enabled" bool,
  "public_traffic" bool,
  "wiki_enabled" bool,
  "retrieved_on" timestamp,
  "should_archive_posts" bool,
  "should_show_media_in_comments_setting" bool,
  "show_media" bool,
  "show_media_preview" bool,
  "user_flair_enabled_in_sr" bool,
  "user_flair_position" varchar,
  "user_flair_text" varchar,
  "user_flair_type" varchar,
  "user_sr_theme_enabled" bool,
  "videostream_links_count" integer
);

CREATE TABLE "subreddit_metadata" (
  "display_name" varchar PRIMARY KEY,
  "earliest_comment_at" timestamp,
  "earliest_post_at" timestamp,
  "num_comments" integer,
  "num_comments_updated_at" timestamp,
  "num_posts" integer,
  "num_posts_updated_at" timestamp
);

CREATE TABLE "subreddit_settings" (
  "display_name" varchar PRIMARY KEY,
  "accept_followers" bool,
  "accounts_active" integer,
  "accounts_active_is_fuzzed" bool,
  "advertiser_category" varchar,
  "all_original_content" bool,
  "allow_prediction_contributors" bool,
  "can_assign_link_flair" bool,
  "can_assign_user_flair" bool,
  "collapse_deleted_comments" bool,
  "comment_score_hide_mins" integer,
  "disable_contributor_requests" bool,
  "has_menu_widget" bool,
  "original_content_tag_enabled" bool,
  "prediction_leaderboard_entry_type" integer,
  "submission_type" varchar,
  "submit_link_label" varchar,
  "submit_text" varchar,
  "submit_text_html" varchar,
  "submit_text_label" varchar,
  "suggested_comment_sort" varchar,
  "notification_level" varchar
);

CREATE TABLE "subreddit_media" (
  "display_name" varchar PRIMARY KEY,
  "banner_background_color" varchar,
  "banner_background_image" varchar,
  "banner_img" varchar,
  "banner_size" varchar,
  "community_icon" varchar,
  "header_img" varchar,
  "header_size" varchar,
  "header_title" varchar,
  "icon_img" varchar,
  "icon_size" varchar,
  "key_color" varchar,
  "mobile_banner_image" varchar,
  "primary_color" varchar
);

CREATE TABLE "subreddit_permissions" (
  "display_name" varchar PRIMARY KEY,
  "free_form_reports" bool,
  "link_flair_enabled" bool,
  "link_flair_position" varchar,
  "user_can_flair_in_sr" bool,
  "user_flair_background_color" varchar,
  "user_flair_css_class" varchar,
  "user_flair_template_id" varchar,
  "user_flair_text_color" varchar
);

CREATE TABLE "subreddit_comment_media" (
  "display_name" varchar,
  "media_type" varchar,
  PRIMARY KEY ("display_name", "media_type")
);

CREATE TABLE "subreddit_rules" (
  "subreddit" varchar PRIMARY KEY,
  "created_utc" timestamp,
  "description" varchar,
  "kind" varchar,
  "priority" integer,
  "short_name" varchar,
  "violation_reason" varchar
);

CREATE TABLE "banned" (
  "author_fullname" varchar,
  "subreddit_id" varchar,
  "banned_at_utc" timestamp,
  "banned_by" varchar,
  PRIMARY KEY ("author_fullname", "subreddit_id")
);

CREATE TABLE "comment" (
  "id" varchar PRIMARY KEY,
  "parent_id" varchar,
  "body" varchar,
  "author_fullname" varchar,
  "created_utc" timestamp,
  "archived" bool,
  "downs" integer,
  "ups" integer,
  "edited" bool,
  "hidden" bool,
  "locked" bool,
  "permalink" varchar,
  "removal_reason" varchar,
  "removed_by" varchar,
  "score" integer,
  "send_replies" bool,
  "spoiler" bool,
  "subreddit" varchar,
  "thumbnail" varchar,
  "thumbnail_height" integer,
  "thumbnail_width" integer,
  "title" varchar,
  "total_awards_received" integer,
  "top_awarded_type" varchar,
  "pinned" bool,
  "contest_mode" bool
);

CREATE TABLE "wiki" (
  "path" varchar PRIMARY KEY,
  "subreddit" varchar,
  "content" varchar,
  "retrieved_on" timestamp,
  "revision_date" date
);

CREATE TABLE "revision_wiki" (
  "path" varchar PRIMARY KEY,
  "revision_author" varchar,
  "revision_author_id" varchar,
  "revision_reason" varchar
);

COMMENT ON COLUMN "post"."selftext" IS 'Body of post';

COMMENT ON COLUMN "post"."top_awarded_type" IS 'Can be NULL';

COMMENT ON COLUMN "comment"."removal_reason" IS 'Can be NULL';

COMMENT ON COLUMN "comment"."removed_by" IS 'Can be NULL';

COMMENT ON COLUMN "comment"."top_awarded_type" IS 'Can be NULL';

COMMENT ON COLUMN "revision_wiki"."revision_author" IS 'Can be NULL';

COMMENT ON COLUMN "revision_wiki"."revision_author_id" IS 'Can be NULL';

COMMENT ON COLUMN "revision_wiki"."revision_reason" IS 'Can be null';

ALTER TABLE "post" ADD FOREIGN KEY ("author_fullname") REFERENCES "author" ("author_fullname");

ALTER TABLE "post" ADD FOREIGN KEY ("subreddit") REFERENCES "subreddit" ("display_name");

ALTER TABLE "subreddit_rules" ADD FOREIGN KEY ("subreddit") REFERENCES "subreddit" ("display_name");

ALTER TABLE "banned" ADD FOREIGN KEY ("author_fullname") REFERENCES "author" ("author_fullname");

ALTER TABLE "comment" ADD FOREIGN KEY ("parent_id") REFERENCES "post" ("id");

ALTER TABLE "comment" ADD FOREIGN KEY ("author_fullname") REFERENCES "author" ("author_fullname");

ALTER TABLE "subreddit" ADD FOREIGN KEY ("display_name") REFERENCES "subreddit_metadata" ("display_name");

ALTER TABLE "subreddit" ADD FOREIGN KEY ("display_name") REFERENCES "subreddit_settings" ("display_name");

ALTER TABLE "subreddit" ADD FOREIGN KEY ("display_name") REFERENCES "subreddit_media" ("display_name");

ALTER TABLE "subreddit" ADD FOREIGN KEY ("display_name") REFERENCES "subreddit_permissions" ("display_name");

CREATE TABLE "subreddit_comment_media_subreddit" (
  "subreddit_comment_media_display_name" varchar,
  "subreddit_display_name" varchar,
  PRIMARY KEY ("subreddit_comment_media_display_name", "subreddit_display_name")
);

ALTER TABLE "subreddit_comment_media_subreddit" ADD FOREIGN KEY ("subreddit_comment_media_display_name") REFERENCES "subreddit_comment_media" ("display_name");

ALTER TABLE "subreddit_comment_media_subreddit" ADD FOREIGN KEY ("subreddit_display_name") REFERENCES "subreddit" ("display_name");


ALTER TABLE "wiki" ADD FOREIGN KEY ("subreddit") REFERENCES "subreddit" ("display_name");

ALTER TABLE "wiki" ADD FOREIGN KEY ("path") REFERENCES "revision_wiki" ("path");

ALTER TABLE "banned" ADD FOREIGN KEY ("subreddit_id") REFERENCES "subreddit" ("id");
