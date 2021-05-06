package com.iplDashBoard.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import java.time.LocalDate;

public class MatchProcessor implements ItemProcessor<MatchInput, MatchOutput> {

     private static final Logger log = LoggerFactory.getLogger(MatchProcessor.class);

        @Override
        public MatchOutput process(final MatchInput matchInput) throws Exception {

            MatchOutput matchOutput = new MatchOutput();
            matchOutput.setId(Integer.parseInt(matchInput.getId()));
            matchOutput.setCity(matchInput.getCity());
            matchOutput.setDate(LocalDate.parse(matchInput.getDate()));
            matchOutput.setPlayerOfMatch(matchInput.getPlayer_of_match());
            matchOutput.setVenue(matchInput.getVenue());

            String firstInningsTeam, secondInningsTeam;
            if ("bat".equals(matchInput.getToss_decision())){
                firstInningsTeam = matchInput.getToss_winner();
                secondInningsTeam = matchInput.getToss_winner().equals(matchInput.getTeam1())
                        ? matchInput.getTeam2() : matchInput.getTeam1();
            }else {
                secondInningsTeam = matchInput.getToss_winner();
                firstInningsTeam = matchInput.getToss_winner().equals(matchInput.getTeam2())
                        ? matchInput.getTeam1() : matchInput.getTeam2();
            }

            matchOutput.setTeam1(firstInningsTeam);
            matchOutput.setTeam2(secondInningsTeam);
            matchOutput.setTossWinner(matchInput.getToss_winner());
            matchOutput.setTossDecision(matchInput.getToss_decision());
            matchOutput.setWinner(matchInput.getWinner());
            matchOutput.setResult(matchInput.getResult());
            matchOutput.setResultMargin(matchInput.getResult_margin());
            matchOutput.setUmpire1(matchInput.getUmpire1());
            matchOutput.setUmpire2(matchInput.getUmpire2());
            return matchOutput;
        }
}
