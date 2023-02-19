CREATE TABLE IF NOT EXISTS public.news (
	id INT generated always as identity,
	createdAt TIMESTAMP not null default NOW(),
	headline text not null,
	"date" TIMESTAMP not null,
	region varchar(16) not null,
	"source" text not null,
	unique (headline),
	primary key (id)
);

CREATE TABLE IF NOT EXISTS public.sentiments (
	id INT generated always as identity,
	"label" varchar(16) not null,
	score float8 NOT NULL,
	news_id INT references public.news(id),
	unique (news_id),
	unique (id),
	primary key (id)
);