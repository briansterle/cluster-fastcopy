FROM amazoncorretto:11

ADD cluster-fastcopy /bin/

CMD [ "/bin/cluster-fastcopy" ]
