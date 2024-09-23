select 
ARRAY_SORT(ARRAY_CONSTRUCT(rb01.rb_id, rb02.rb_id, flex.flex_id, dst.dst_id)) AS sorted_combo    
,rb01.rb_id as rb01_id
    ,rb01.rb as rb01
    ,rb01.rb_pos as rb01_pos
    ,rb01.rb_team as rb01_team
    ,rb01.rb_opp as rb01_opp
    ,rb01.rb_game as rb01_game
    ,rb02.rb_id as rb02_id
    ,rb02.rb as rb02
    ,rb02.rb_pos as rb02_pos
    ,rb02.rb_team as rb02_team
    ,rb02.rb_opp as rb02_opp
    ,rb02.rb_game as rb02_game
    ,flex.flex_id as flex_id
    ,flex.flex
    ,flex.flex_pos as flex_pos
    ,flex.flex_team as flex_team
    ,flex.flex_opp as flex_opp
    ,flex.flex_game as flex_game
    ,dst.dst_id
    ,dst.dst
    ,dst.dst_pos as dst_pos
    ,dst.dst_team as dst_team
    ,dst.dst_opp as dst_opp
    ,dst.dst_game as dst_game

    ,rb01.rb_sal + rb02.rb_sal + flex.flex_sal + dst.dst_sal as sal
    ,rb01.rb_fpts + rb02.rb_fpts + flex.flex_fpts + dst.dst_fpts as fpts
    ,rb01.rb_val + rb02.rb_val + flex.flex_val + dst.dst_val as val
    
    
from {{ref("qualifiedRBs")}} rb01
join {{ref("qualifiedRBs")}} rb02
    on rb01.rb_team != rb02.rb_team

join {{ref("qualifiedFLEXs")}} flex
    on rb01.rb_team != flex.flex_team
    and rb02.rb_team != flex.flex_team

join {{ref("qualifiedDSTs")}} dst
    on rb01.rb_opp != dst.dst_team
    and rb02.rb_opp != dst.dst_team
    and flex.flex_opp != dst.dst_team