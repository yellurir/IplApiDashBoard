package com.iplDashBoard.controller;

import com.iplDashBoard.batch.Team;
import com.iplDashBoard.repository.MatchRepository;
import com.iplDashBoard.repository.TeamRepository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TeamController {

    @Autowired
    private TeamRepository teamRepository;

    @Autowired
    private MatchRepository matchRepository;

    /*@Autowired
    public TeamController(TeamRepository teamRepository){
        this.teamRepository = teamRepository;
    }
*/
    @GetMapping("/teams/{teamName}")
    public Team getTeamName(@PathVariable String teamName){
        Team team = teamRepository.findByTeamName(teamName);
        team.setMatches(matchRepository.findLatestMatchesByTeam(teamName, 3));
        return team;
    }

    @GetMapping("/hello")
    public String hello(){
        return "hi";
    }

}
