-- Готівкові завершені поїздки: чайових у даних нема, тож tip_amount має бути NULL, а не 0.
-- Ловить «виправлення» coalesce(tip, 0) у моделі rides: воно перетворює «невідомо» на «нуль»
-- і тихо псує і суми, і середні.
select ride_id, payment_method, tip_amount
from {{ ref('rides') }}
where status = 'completed'
  and payment_method = 'cash'
  and tip_amount is not null