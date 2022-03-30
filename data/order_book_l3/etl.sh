sqlite3 './data.db' -csv "
	select
	    json_extract(json(message), '$.market') as market,
	    json_extract(json(message), '$.slot') as slot,
	    message
	from entries
" | psql -d mangolorians -c "copy order_book_l3 from stdin csv"
