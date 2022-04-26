-- target=variables
select 1 as a, '2' as b

-- target=variables
select
	${a} as a
	, ${b} as b
	, 1${a} as a1
	, ${a} + ${b} as ab

-- target=log.variables
select ${a} as a, ${b} as b, ${a1} as a1, ${ab} as ab
