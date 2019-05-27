1. Game Totals - Shots Attempt For

select COUNT(cast(index as int)) as count, "Game", "Scouted Player" from hitmen_hockey_03062019 
where "ClipType" like 'Shot Attempt For' 
Group By "Game", "Scouted Player"
Order By "Game"

2. Game Totals - Shots on Goal

select COUNT(cast(index as int)) as count, "Game", "Scouted Player" from hitmen_hockey_03062019 
where "ClipType" like 'Shot Attempt For' and "SAGRADE" like '%2%'
Group By "Game", "Scouted Player"
Order By "Game"

3. Game Totals - Chance For

select COUNT(cast(index as int)) as count, "Game", "Scouted Player" from hitmen_hockey_03062019 
where "ClipType" like 'Chance For'
Group By "Game", "Scouted Player"
Order By "Game"

4. Game Totals - Chance Created

select COUNT(cast(index as int)) as count, "Game", "CHANCE CREATE" as Player from hitmen_hockey_03062019 
where "CHANCE CREATE" IS NOT NULL and "CHANCE CREATE" != 'ChanceCreate'
Group By "Game", "CHANCE CREATE"
Order By "Game"