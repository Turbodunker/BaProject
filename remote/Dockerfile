# Use ubuntu as described by Docker documentation
FROM ubuntu:latest
#ENV PATH="$PATH:/etc/profile"

# Install dependencies
RUN apt-get update && apt-get install -y \
	openssh-client \
	openssh-server \
    	sshfs \
	vim \
	tree \
	python3-pip \
	git \
	cmake
	


RUN mkdir /run/sshd
	
# Create a non-root user
RUN useradd -ms /bin/bash shodan
EXPOSE 22
ENV PATH="${PATH}:/home/shodan/.local/bin"
#RUN echo "export PATH=/usr/local/bin:/usr/bin:/home/shodan/.local/bin:$PATH" >> /home/shodan/.bashrc
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

# Git depdendencies
RUN git clone https://github.com/dmpelt/foam_ct_phantom
RUN chown -R shodan:shodan /home/shodan/foam_ct_phantom
RUN chown shodan:shodan /usr/local/lib/python3.10/dist-packages/
# Switch to the non-root user
USER shodan
RUN pip install --upgrade pip ipython ipykernel --user
RUN ipython kernel install --name "python3" --user
RUN pip install scikit-build --user
RUN pip install scikit-learn --user
RUN pip install scikit-image==0.18.3 --user
RUN pip install h5py --user
RUN pip install tqdm --user
RUN pip install pathlib2 --user

WORKDIR /home/shodan/foam_ct_phantom/
RUN ls 
RUN python3 /home/shodan/foam_ct_phantom/setup.py install
WORKDIR /home/shodan
RUN python3 -m pip install papermill --user
#Fetcj and call the docker-entrypoint script
COPY docker-entrypoint.sh /home/shodan/docker-entrypoint.sh
#CMD "env"
ENTRYPOINT ["./docker-entrypoint.sh"]
