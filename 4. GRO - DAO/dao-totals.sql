WITH
    team AS (
        SELECT new.contributor AS "contributor",
               sum(coalesce(new.amount/1e18, 0)) AS "allocated",
               sum(coalesce(claim.amount/1e18, 0)) AS "claimed",
               sum(coalesce(withdraw.amount/1e18, 0)) AS "withdrawn",
               count(new.contributor) AS "positions",
               count(stop.unlocked) AS "locked"
        FROM gro."GROTeamVesting_evt_LogNewVest" new
            LEFT JOIN gro."GROTeamVesting_evt_LogWithdrawal" withdraw
                ON new."contributor" = withdraw."account"
            LEFT JOIN gro."GROTeamVesting_evt_LogClaimed" claim
                ON new."contributor" = claim."contributor"
            LEFT JOIN gro."GROTeamVesting_evt_LogStoppedVesting" stop
                ON new."contributor" = stop."contributor"
        GROUP BY 1
    ),
    team_totals AS (
        SELECT sum(allocated) AS "total allocated",
               sum(claimed) AS "total claimed",
               sum(withdrawn) AS "total withdrawn",
               count(distinct(contributor)) AS "total contributors",
               sum(CASE WHEN positions = locked THEN 1 ELSE 0 END) AS "total inactive"
          FROM team
    )

SELECT * 
FROM team_totals
