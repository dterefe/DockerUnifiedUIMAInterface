package org.texttechnologylab.duui.timelines;

import org.texttechnologylab.duui.ems.DUUIActor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Stream;

@Aspect
public class DUUIPhaseAspect {
    private static final DUUIDispatcher DISPATCHER = new DUUIDispatcher();

    @Around("execution(org.texttechnologylab.duui.timelines.DUUIFlow+ *(..)) && @annotation(phase)")
    public Object aroundFlow(ProceedingJoinPoint joinPoint, Phase phase) {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        Object owner = joinPoint.getTarget();
        Object[] arguments = joinPoint.getArgs();
        DUUIDispatcher dispatcher = DISPATCHER;
        if (owner != null) {
            try {
                Method dispatcherMethod = owner.getClass().getMethod("dispatcher");
                if (DUUIDispatcher.class.isAssignableFrom(dispatcherMethod.getReturnType())) {
                    dispatcher = (DUUIDispatcher) dispatcherMethod.invoke(owner);
                }
            } catch (ReflectiveOperationException ignored) {
                dispatcher = DISPATCHER;
            }
        }
        return dispatcher.dispatchFlowResult(new DUUIDispatcher.Invocation<>(
                phase,
                method,
                owner,
                Stream.concat(
                        owner instanceof DUUIActor actor ? Stream.of(actor) : Stream.empty(),
                        Arrays.stream(arguments == null ? new Object[0] : arguments)
                                .filter(DUUIActor.class::isInstance)
                                .map(DUUIActor.class::cast)
                ).toList(),
                Arrays.asList(arguments == null ? new Object[0] : arguments),
                () -> {
                    try {
                        return (DUUIFlow<?>) joinPoint.proceed();
                    } catch (Exception exception) {
                        throw exception;
                    } catch (Throwable throwable) {
                        throw new RuntimeException(throwable);
                    }
                }
        ));
    }
}
