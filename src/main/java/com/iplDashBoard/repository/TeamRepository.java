package com.iplDashBoard.repository;

import com.iplDashBoard.batch.Team;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TeamRepository extends CrudRepository<Team,Long> {

    Team findByTeamName(String teamName);

}
