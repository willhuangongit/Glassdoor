select comp_id, company_name, type_of_org, industry, 
	year_founded, size, revenue
from overviews 
where company_name like "Microsoft" 
limit 1;

select category, count(category)
from jobs
where 
    comp_id = (
    	select comp_id 
    	from overviews
    	where company_name = "TD"
    )
group by category
order by count(category) desc
limit 10;


select o.company_name, o.industry, 
    o.size, o.revenue,
    f.overall_stars_avg
from overviews o
join
	(
		select o.comp_id, 
		    avg(r.overall_stars) overall_stars_avg,
		    count(r.overall_stars) overall_stars_n
		from overviews o
		join reviews r
		on o.comp_id = r.comp_id
		where 
		    o.industry in (
			'Banks & Credit Unions',
			'Financial Analytics & Research',
			'Financial Transaction Processing',
			'Investment Banking & Asset Management'
		    )
		group by comp_id
	) as f
on o.comp_id = f.comp_id
where f.overall_stars_n >= 1000
order by f.overall_stars_avg desc
limit 10;
