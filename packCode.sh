
wget https://github.com/danbarto/tW_scattering/archive/WH_allHadronic.zip
unzip WH_allHadronic.zip
mv tW_scattering-WH_allHadronic tW_scattering

tar -czf tW_scattering.tar.gz tW_scattering

rm -rf tW_scattering
rm WH_allHadronic.zip

mv tW_scattering.tar.gz Tools/analysis.tar.gz
