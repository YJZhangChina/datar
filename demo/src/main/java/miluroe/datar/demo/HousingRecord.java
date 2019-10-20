package miluroe.datar.demo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class HousingRecord {

    private float longitude;
    private float latitude;
    private float housingMedianAge;
    private float totalRooms;
    private float totalBedrooms;
    private float population;
    private float households;
    private float medianIncome;
    private float medianHouseValue;
    private String oceanProximity;

}
