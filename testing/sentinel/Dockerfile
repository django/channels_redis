FROM redis:3

EXPOSE 26379
ADD sentinel.conf /etc/redis/sentinel.conf
RUN chown redis:redis /etc/redis/sentinel.conf
CMD docker-entrypoint.sh redis-server /etc/redis/sentinel.conf --sentinel
