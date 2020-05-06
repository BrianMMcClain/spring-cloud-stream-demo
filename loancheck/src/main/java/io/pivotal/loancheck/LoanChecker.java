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

  /**
   * This method takes in a stream of Loan applications and determines if they
   * are approved or declined. These loan applications are presented to the code
   * through the Flux<Loan> object.
   * 
   * @return a Tuple of Flux<Loan> objects, which represents two streams of Loan objects,
   * one for approved loans and one for declined loans
   */
  @Bean
  public static Function<Flux<Loan>, Tuple2<Flux<Loan>, Flux<Loan>>> checkAndSortLoans() {
    return flux -> {

      // This flux contains the stream of Loan applications as they come in to our service
      Flux<Loan> connectedFlux = flux.publish().autoConnect(2);
      
      // These processors will be responsible for emitting approved/declined loans to
      // the two Flux objects that wil be returned
			UnicastProcessor approved = UnicastProcessor.create();
			UnicastProcessor declined = UnicastProcessor.create();
      
      // Loans for an amount more than MAX_AMOUNT get marked as declined, logged, and then sent to the declined processor
      Flux<Loan> declinedFlux = connectedFlux.filter(loan -> loan.getAmount() > MAX_AMOUNT).doOnNext(loan -> {
        loan.setStatus(Statuses.DECLINED.name());
        log.info("{} {} for ${} for {}", loan.getStatus(), loan.getUuid(), loan.getAmount(), loan.getName());
        declined.onNext(loan);
      });
      
      // Loans for an amount less than MAX_AMOUNT get marked as approved, logged, and then sent to the approved processor
      Flux<Loan> approvedFlux = connectedFlux.filter(loan -> loan.getAmount() <= MAX_AMOUNT).doOnNext(loan -> {
        loan.setStatus(Statuses.APPROVED.name());
        log.info("{} {} for ${} for {}", loan.getStatus(), loan.getUuid(), loan.getAmount(), loan.getName());
        approved.onNext(loan);
      });

      // Return two Loan Fluxes, one subscribed to the approved processor and one to the declined processor
			return Tuples.of(Flux.from(approved).doOnSubscribe(x -> approvedFlux.subscribe()), Flux.from(declined).doOnSubscribe(x -> declinedFlux.subscribe()));
		};
  }

  private static final <T> Message<T> message(T val) {
    return MessageBuilder.withPayload(val).build();
  }
}
