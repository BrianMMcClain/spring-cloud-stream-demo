package io.pivotal.loancheck;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Component
public class LoanChecker {

  public static final Logger log = LoggerFactory.getLogger(LoanChecker.class);
  private static final Long MAX_AMOUNT = 10000L;

  @Bean
	public static Function<Flux<Loan>, Tuple2<Flux<Loan>, Flux<Loan>>> checkAndSortLoans() {
		return flux -> {
			Flux<Loan> connectedFlux = flux.publish().autoConnect(2);
			UnicastProcessor approved = UnicastProcessor.create();
			UnicastProcessor declined = UnicastProcessor.create();
      
      Flux<Loan> declinedFlux = connectedFlux.filter(loan -> loan.getAmount() > MAX_AMOUNT).doOnNext(loan -> {
        loan.setStatus(Statuses.DECLINED.name());
        log.info("{} {} for ${} for {}", loan.getStatus(), loan.getUuid(), loan.getAmount(), loan.getName());
        declined.onNext(loan);
      });
      
      Flux<Loan> approvedFlux = connectedFlux.filter(loan -> loan.getAmount() <= MAX_AMOUNT).doOnNext(loan -> {
        loan.setStatus(Statuses.APPROVED.name());
        log.info("{} {} for ${} for {}", loan.getStatus(), loan.getUuid(), loan.getAmount(), loan.getName());
        approved.onNext("APPROVED: " + loan);
      });

			return Tuples.of(Flux.from(approved).doOnSubscribe(x -> approvedFlux.subscribe()), Flux.from(declined).doOnSubscribe(x -> declinedFlux.subscribe()));
		};
  }

  // public void checkAndSortLoans(Loan loan) {
  //   log.info("{} {} for ${} for {}", loan.getStatus(), loan.getUuid(), loan.getAmount(), loan.getName());

  //   if (loan.getAmount() > MAX_AMOUNT) {
  //     loan.setStatus(Statuses.DECLINED.name());
  //     processor.declined().send(message(loan));
  //   } else {
  //     loan.setStatus(Statuses.APPROVED.name());
  //     processor.approved().send(message(loan));
  //   }

  //   log.info("{} {} for ${} for {}", loan.getStatus(), loan.getUuid(), loan.getAmount(), loan.getName());

  // }

  private static final <T> Message<T> message(T val) {
    return MessageBuilder.withPayload(val).build();
  }
}
