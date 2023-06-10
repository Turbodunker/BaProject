# Use ubuntu as described by Docker documentation
FROM ubuntu:latest

# Install dependencies
RUN apt-get update && apt-get install -y \
	openssh-client \
	openssh-server \
    	sshfs \
	vim \
	tree

RUN mkdir /run/sshd
	
# Create a non-root user
RUN useradd -ms /bin/bash shodan
EXPOSE 22

# Setup shh
COPY remote.key.pub /home/shodan/.ssh/remote.key.pub
COPY remote.key /home/shodan/.ssh/remote.key
COPY known_hosts /home/shodan/.ssh/known_hosts
RUN ssh-keyscan 192.168.0.4 >> /home/shodan/.ssh/known_hosts
RUN chown shodan:shodan /etc/fuse.conf
RUN echo user_allow_other >> /etc/fuse.conf

WORKDIR /home/shodan
RUN chmod 700 .ssh && \
    chmod 600 .ssh/remote.key && \
    touch .ssh/known_hosts && \
    chmod 644 .ssh/known_hosts && \
    cat .ssh/remote.key.pub >> .ssh/authorized_keys && \
    chmod 600 .ssh/authorized_keys && \
    chown shodan:shodan -R .ssh

RUN echo "IdentityFile ~/.ssh/remote.key" >> /etc/ssh/sshd_config

RUN mkdir -p /home/shodan
RUN mkdir -p /jail/shodan
RUN chown shodan:shodan /jail/shodan
RUN chown shodan:shodan /home/shodan

# Switch to the non-root user
USER shodan

RUN cd /home/shodan/
#Fetcj and call the docker-entrypoint script
COPY docker-entrypoint.sh /home/shodan/docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]
