FROM registry.access.redhat.com/ubi8/ubi-minimal

ADD kafka-topic-view /kafka-topic-view
ADD static /static

USER 1001