copy (
  select distinct
      referree_mango_account,
      referrer_mango_account
  from transactions_v3.referral_fee_accrual
  where mango_group = '98pjRuQjK3qA6gXts96PqZT4Ze5QmnCmt3QYjhbUSPue'
) to stdout csv header;

