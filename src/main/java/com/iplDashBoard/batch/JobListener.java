package com.iplDashBoard.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import javax.persistence.EntityManager;
import javax.transaction.Transactional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class JobListener extends JobExecutionListenerSupport {
    
    private static final Logger log =
            LoggerFactory.getLogger(JobListener.class);
    private final EntityManager em;

    @Autowired
        public JobListener(EntityManager em) {
            this.em = em;
        }

        @Override
        @Transactional
        public void afterJob(JobExecution jobExecution) {
            if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
                log.info("!!! JOB FINISHED! Time to verify the results");
                List<Team> team = new ArrayList<>();

                List<Object[]> resultList1 = em.createQuery("select distinct m.team1,count(*) " +
                        "from MatchOutput m " +
                        "group by m.team1 ", Object[].class).getResultList();
                for (int i = 0; i < resultList1.size(); i++) {
                    team.add(new Team((String) resultList1.get(i)[0], (long) resultList1.get(i)[1]));
                }

                List<Object[]> resultList2 = em.createQuery("select distinct m.team2,count(*) " +
                        "from MatchOutput m " +
                        "group by m.team2 ", Object[].class).getResultList();

                for (int i = 0; i < resultList2.size(); i++) {
                    /*if (resultList2.get(i)[0].equals(team1.get(i)))*/
                    if (team.get(i).getTeamName().equals(resultList2.get(i)[0])) {
                        team.get(i).setTotalMatches(team.get(i).getTotalMatches() + (long) resultList2.get(i)[1]);
                    } else {
                        team.add(new Team((String) resultList1.get(i)[0], (long) resultList1.get(i)[1]));
                    }
                }

                List<Object[]> totalWins = em.createQuery("select distinct m.winner,count(*) " +
                        " from MatchOutput m " +
                        " group by winner", Object[].class).getResultList();

                for (int i = 0; i < totalWins.size() - 1; i++) {
                    if (totalWins.get(i)[0] != null) {
                        if (team.get(i).getTeamName().equals(totalWins.get(i)[0])) {
                            team.get(i).setTotalWins((long) totalWins.get(i)[1]);
                        }
                    }
                }
                Map<String, Team> map = new HashMap<>();
                for (int i = 0; i < team.size(); i++) {
                    map.put(team.get(i).getTeamName(), team.get(i));
                    //System.out.println(map.get(team1.get(i).getTeamName()));
                }

                map.values().forEach(team1 -> em.persist(team1));
            }
        }
}