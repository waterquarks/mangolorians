sqlite3 './data.db' -csv "
	select
	    json_extract(json(message), '$.market') as market,
	    json_extract(json(message), '$.slot') as slot,
	    json_extract(json(message), '$.timestamp') as timestamp,
	    json_extract(json(message), '$.type') as type,
	    message
	from entries
" | psql -d mangolorians -c "copy order_book_l3 from stdin csv"
